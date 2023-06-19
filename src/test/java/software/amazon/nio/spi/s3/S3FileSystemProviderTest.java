/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
public class S3FileSystemProviderTest {

    S3FileSystemProvider provider;
    S3FileSystem fs;
    String pathUri = "s3://foo/baa";
    @Mock
    S3AsyncClient mockClient;

    @BeforeEach
    public void init() {
        provider = new S3FileSystemProvider();
        fs = new S3FileSystem("s3://mybucket", provider);
        lenient().when(mockClient.headObject(any(Consumer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
    }

    @Test
    public void getClientStore() {
        S3ClientStore s;
        assertNotNull(s = provider.getClientStore());
        assertSame(s, provider.getClientStore());
    }

    @Test
    public void getScheme() {
        assertEquals("s3", provider.getScheme());
    }

    @Test
    public void newFileSystem() {
        URI uri = URI.create(pathUri);
        final FileSystem fileSystem = provider.newFileSystem(uri, Collections.emptyMap());
        assertTrue(fileSystem instanceof S3FileSystem);
    }

    @Test
    public void getFileSystem() {
        assertNotNull(provider.getFileSystem(URI.create(pathUri)));
    }

    @Test
    public void getPath() {
        assertNotNull(provider.getPath(URI.create(pathUri)));
    }

    @Test
    public void newByteChannel() throws IOException {
        final SeekableByteChannel channel = provider.newByteChannel(mockClient, Paths.get(URI.create(pathUri)), Collections.singleton(StandardOpenOption.READ));
        assertNotNull(channel);
        assertTrue(channel instanceof S3SeekableByteChannel);
    }

    @Test
    public void newDirectoryStream() throws ExecutionException, InterruptedException {

        S3Object object1 = S3Object.builder().key("key1").build();
        S3Object object2 = S3Object.builder().key("foo/key2").build();

        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).build()));

        final DirectoryStream<Path> stream = provider.newDirectoryStream(mockClient, Paths.get(URI.create(pathUri)), entry -> true);
        assertNotNull(stream);
        assertEquals(2, countDirStreamItems(stream));

        final DirectoryStream<Path> filteredStream = provider.newDirectoryStream(mockClient, Paths.get(URI.create(pathUri)),
                entry -> entry.endsWith("key2"));
        assertNotNull(filteredStream);
        assertEquals(1, countDirStreamItems(filteredStream));
    }

    private int countDirStreamItems(DirectoryStream<Path> stream) {
        AtomicInteger count = new AtomicInteger(0);
        stream.iterator().forEachRemaining(item -> count.incrementAndGet());
        return count.get();
    }

    @Test
    public void createDirectory() throws ExecutionException, InterruptedException {
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                PutObjectResponse.builder().build()));

        provider.createDirectory(mockClient, fs.getPath("/foo/baa/baz/"));

        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockClient, times(1)).putObject(argumentCaptor.capture(), any(AsyncRequestBody.class));
        assertEquals("mybucket", argumentCaptor.getValue().bucket());
        assertEquals("foo/baa/baz/", argumentCaptor.getValue().key());
    }

    @Test
    public void delete() throws ExecutionException, InterruptedException {
        S3Object object1 = S3Object.builder().key("foo/key1").build();
        S3Object object2 = S3Object.builder().key("foo/subpath/key2").build();
        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        provider.delete(mockClient, fs.getPath("/foo"));

        ArgumentCaptor<DeleteObjectsRequest> argumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(argumentCaptor.capture());
        DeleteObjectsRequest captorValue = argumentCaptor.getValue();
        assertEquals("mybucket", captorValue.bucket());
        List<String> keys = captorValue.delete().objects().stream().map(objectIdentifier -> objectIdentifier.key()).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("foo/key1"));
        assertTrue(keys.contains("foo/subpath/key2"));
    }

    @Test
    public void copy() throws IOException, ExecutionException, InterruptedException {
        S3Object object1 = S3Object.builder().key("foo/key1").build();
        S3Object object2 = S3Object.builder().key("foo/subpath/key2").build();
        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        HeadObjectRequest headObjectRequest1 = HeadObjectRequest.builder().bucket("mybucket").key("baa/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));

        S3Path foo = fs.getPath("/foo");
        S3Path baa = fs.getPath("/baa");
        assertThrows(FileAlreadyExistsException.class, () -> provider.copy(mockClient, foo, baa));
        provider.copy(mockClient, foo, baa, StandardCopyOption.REPLACE_EXISTING);

        ArgumentCaptor<CopyObjectRequest> argumentCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockClient, times(2)).copyObject(argumentCaptor.capture());
        List<CopyObjectRequest> requestValues = argumentCaptor.getAllValues();
        assertEquals("mybucket", requestValues.get(0).sourceBucket());
        assertEquals("foo/key1", requestValues.get(0).sourceKey());
        assertEquals("mybucket", requestValues.get(0).destinationBucket());
        assertEquals("baa/key1", requestValues.get(0).destinationKey());
        assertEquals("mybucket", requestValues.get(1).sourceBucket());
        assertEquals("foo/subpath/key2", requestValues.get(1).sourceKey());
        assertEquals("mybucket", requestValues.get(1).destinationBucket());
        assertEquals("baa/subpath/key2", requestValues.get(1).destinationKey());
    }

    @Test
    public void move() throws IOException, ExecutionException, InterruptedException {
        S3Object object1 = S3Object.builder().key("foo/key1").build();
        S3Object object2 = S3Object.builder().key("foo/subpath/key2").build();
        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        HeadObjectRequest headObjectRequest1 = HeadObjectRequest.builder().bucket("mybucket").key("baa/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        S3Path foo = fs.getPath("/foo");
        S3Path baa = fs.getPath("/baa");
        assertThrows(FileAlreadyExistsException.class, () -> provider.copy(mockClient, foo, baa));
        provider.move(mockClient, foo, baa, StandardCopyOption.REPLACE_EXISTING);

        ArgumentCaptor<CopyObjectRequest> argumentCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockClient, times(2)).copyObject(argumentCaptor.capture());
        List<CopyObjectRequest> requestValues = argumentCaptor.getAllValues();
        assertEquals("mybucket", requestValues.get(0).sourceBucket());
        assertEquals("foo/key1", requestValues.get(0).sourceKey());
        assertEquals("mybucket", requestValues.get(0).destinationBucket());
        assertEquals("baa/key1", requestValues.get(0).destinationKey());
        assertEquals("mybucket", requestValues.get(1).sourceBucket());
        assertEquals("foo/subpath/key2", requestValues.get(1).sourceKey());
        assertEquals("mybucket", requestValues.get(1).destinationBucket());
        assertEquals("baa/subpath/key2", requestValues.get(1).destinationKey());
        ArgumentCaptor<DeleteObjectsRequest> deleteArgumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(deleteArgumentCaptor.capture());
        List<String> keys = deleteArgumentCaptor.getValue().delete().objects().stream().map(objectIdentifier -> objectIdentifier.key()).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("foo/key1"));
        assertTrue(keys.contains("foo/subpath/key2"));
    }

    @Test
    public void isSameFile() throws IOException {
        S3Path foo = fs.getPath("/foo");
        S3Path baa = fs.getPath("/baa");

        assertFalse(provider.isSameFile(foo, baa));
        assertTrue(provider.isSameFile(foo, foo));

        S3Path alsoFoo = fs.getPath("foo");
        assertTrue(provider.isSameFile(foo, alsoFoo));

        S3Path alsoFoo2 = fs.getPath("./foo");
        assertTrue(provider.isSameFile(foo, alsoFoo2));
    }

    @Test
    public void isHidden() {
        S3Path foo = fs.getPath("/foo");
        //s3 doesn't have hidden files
        S3Path baa = fs.getPath(".baa");

        assertFalse(provider.isHidden(foo));
        assertFalse(provider.isHidden(baa));
    }

    @Test
    public void getFileStore() {
        S3Path foo = fs.getPath("/foo");
        //s3 doesn't have file stores
        assertNull(provider.getFileStore(foo));
    }

    @Test
    public void checkAccessWithoutException() throws IOException, ExecutionException, InterruptedException {

        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));

        S3Path foo = fs.getPath("/foo");
        provider.checkAccess(mockClient, foo, AccessMode.READ);
        provider.checkAccess(mockClient, foo, AccessMode.EXECUTE);
        provider.checkAccess(mockClient, foo);
    }

    @Test
    public void checkAccessWhenAccessDenied() throws IOException, ExecutionException, InterruptedException {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(403).build())
                        .build()));

        S3Path foo = fs.getPath("/foo");
        assertThrows(AccessDeniedException.class, () -> provider.checkAccess(mockClient, foo));
    }

    @Test
    public void checkAccessWhenNoSuchFile() throws IOException, ExecutionException, InterruptedException {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build())
                        .build()));

        S3Path foo = fs.getPath("/foo");
        assertThrows(NoSuchFileException.class, () -> provider.checkAccess(mockClient, foo));
    }

    @Test
    public void checkWriteAccess() throws IOException, ExecutionException, InterruptedException {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));
        provider.checkAccess(mockClient, fs.getPath("foo"), AccessMode.WRITE);
    }

    @Test
    public void getFileAttributeView() {
        S3Path foo = fs.getPath("/foo");
        final BasicFileAttributeView fileAttributeView = provider.getFileAttributeView(foo, BasicFileAttributeView.class);
        assertNotNull(fileAttributeView);
        assertTrue(fileAttributeView instanceof S3FileAttributeView);

        final S3FileAttributeView fileAttributeView1 = provider.getFileAttributeView(foo, S3FileAttributeView.class);
        assertNotNull(fileAttributeView1);
    }

    @Test
    public void getFileAttributeViewIllegalArg() {
        S3Path foo = fs.getPath("/foo");
        assertThrows(IllegalArgumentException.class, () -> provider.getFileAttributeView(foo, FileAttributeView.class));
    }

    @Test
    public void readAttributes() {
        S3Path foo = fs.getPath("/foo");
        final BasicFileAttributes BasicFileAttributes = provider.readAttributes(mockClient, foo, BasicFileAttributes.class);
        assertNotNull(BasicFileAttributes);
        assertTrue(BasicFileAttributes instanceof S3BasicFileAttributes);

        final S3BasicFileAttributes s3BasicFileAttributes = provider.readAttributes(mockClient, foo, S3BasicFileAttributes.class);
        assertNotNull(s3BasicFileAttributes);
    }

    @Test
    public void testReadAttributes() {
        S3Path foo = fs.getPath("/foo");
        S3Path fooDir = fs.getPath("/foo/");

        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.completedFuture(
                HeadObjectResponse.builder()
                        .lastModified(Instant.EPOCH)
                        .contentLength(100L)
                        .eTag("abcdef")
                        .build()));

        Map<String, Object> attributes = provider.readAttributes(mockClient, foo, "*");
        assertTrue(attributes.size() >= 9);

        attributes = provider.readAttributes(mockClient, foo, "lastModifiedTime,size,fileKey");
        assertEquals(3, attributes.size());
        assertEquals(FileTime.from(Instant.EPOCH), attributes.get("lastModifiedTime"));
        assertEquals(100L, attributes.get("size"));

        assertEquals(Collections.emptyMap(), provider.readAttributes(mockClient, fooDir, "*"));
    }

    @Test
    public void setAttribute() {
        S3Path foo = fs.getPath("/foo");
        assertThrows(UnsupportedOperationException.class, () -> provider.setAttribute(foo, "x", "y"));
    }
}