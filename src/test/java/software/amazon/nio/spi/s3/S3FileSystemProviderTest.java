/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import software.amazon.awssdk.services.s3.S3AsyncClient;

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
        lenient().when(mockClient.headObject(anyConsumer())).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
        fs = provider.getFileSystem(URI.create(pathUri), true);
        fs.clientProvider(new FixedS3ClientProvider(mockClient));
    }

    @AfterEach
    public void after() {
       provider.closeFileSystem(fs);
    }

    @Test
    public void getScheme() {
        assertEquals("s3", provider.getScheme());
    }

    @Test
    @DisplayName("newFileSystem(URI, env) should throw")
    public void newFileSystemURI() {
        assertThatThrownBy(
            () -> new S3FileSystemProvider().newFileSystem(URI.create(pathUri), Collections.emptyMap())
        ).isInstanceOf(NotYetImplementedException.class);
    }

    @Test
    @DisplayName("newFileSystem(Path, env) should throw")
    public void newFileSystemPath() {
        assertThatThrownBy(
            () -> new S3FileSystemProvider().newFileSystem(Paths.get(pathUri), Collections.emptyMap())
        ).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void getFileSystem() {
        assertThatCode(() -> provider.getFileSystem(null))
                .as("missing argument check!")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("uri can not be null");

        assertThatCode(() -> provider.getFileSystem(URI.create("s3:///")))
                .as("missing argument check!")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket name cannot be null");
        //
        // A filesystem for pathUri has been created already ;)
        //
        assertSame(fs, provider.getFileSystem(URI.create(pathUri)));

        //
        // New AWS S3 file system
        //
        S3FileSystem cfs = provider.getFileSystem(URI.create("s3://foo2/baa"), true);
        FileSystem gfs = provider.getFileSystem(URI.create("s3://foo2"));
        assertNotSame(fs, gfs); assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://foo2"));
        assertNotSame(fs, gfs); assertSame(cfs, gfs);
        provider.closeFileSystem(cfs);

        //
        // New AWS S3 file system with same bucket but different path
        //
        cfs = provider.getFileSystem(URI.create("s3://foo3"), true);
        gfs = provider.getFileSystem(URI.create("s3://foo3/dir"));
        assertNotSame(fs, gfs); assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://foo3/dir"));
        assertNotSame(fs, gfs); assertSame(cfs, gfs);
        provider.closeFileSystem(cfs);

        assertThrows(
            FileSystemNotFoundException.class, () -> {
                provider.getFileSystem(URI.create("s3://nobucket"));
            }
        );
    }

    @Test
    public void closingFileSystemDiscardsItFromCache() {
        provider.closeFileSystem(fs);

        assertThrows(
            FileSystemNotFoundException.class,
            () -> provider.getFileSystem(URI.create(pathUri))
        );
    }

    @Test
    public void newByteChannel() throws Exception {
        final SeekableByteChannel channel = provider.newByteChannel(Paths.get(URI.create(pathUri)), Collections.singleton(StandardOpenOption.READ));
        assertNotNull(channel);
        assertTrue(channel instanceof S3SeekableByteChannel);
    }

    @Test
    public void newDirectoryStream() {

        S3Object object1 = S3Object.builder().key(pathUri+"/key1").build();
        S3Object object2 = S3Object.builder().key(pathUri+"/key2").build();

        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(new ListObjectsV2Publisher(mockClient,
                ListObjectsV2Request.builder()
                        .bucket(fs.bucketName())
                        .prefix(pathUri + "/")
                        .build())
        );

        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).build()));

        final DirectoryStream<Path> stream = provider.newDirectoryStream(fs.getPath(pathUri+"/"), entry -> true);
        assertNotNull(stream);
        assertEquals(2, countDirStreamItems(stream));
    }

    @Test
    public void pathIteratorForPublisher_withPagination() {
        final ListObjectsV2Publisher publisher = new ListObjectsV2Publisher(mockClient,
                ListObjectsV2Request.builder()
                        .bucket(fs.bucketName())
                        .prefix(pathUri + "/")
                        .build());
        S3Object object1 = S3Object.builder().key(pathUri+"/key1").build();
        S3Object object2 = S3Object.builder().key(pathUri+"/key2").build();
        S3Object object3 = S3Object.builder().key(pathUri+"/").build();

        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(publisher);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2, object3).build()));

        Iterator<Path> pathIterator =
            provider.newDirectoryStream(fs.getPath(pathUri + "/"), path -> true).iterator();

        assertNotNull(pathIterator);
        assertTrue(pathIterator.hasNext());
        assertEquals(fs.getPath(object1.key()), pathIterator.next());
        assertTrue(pathIterator.hasNext());
        assertEquals(fs.getPath(object2.key()), pathIterator.next());
        // object3 should not be present
        assertFalse(pathIterator.hasNext());
    }

    @Test
    public void pathIteratorForPublisher_appliesFilter() {
        final ListObjectsV2Publisher publisher = new ListObjectsV2Publisher(mockClient,
                ListObjectsV2Request.builder()
                        .bucket(fs.bucketName())
                        .prefix(pathUri + "/")
                        .build());
        S3Object object1 = S3Object.builder().key(pathUri+"/key1").build();
        S3Object object2 = S3Object.builder().key(pathUri+"/key2").build();
        S3Object object3 = S3Object.builder().key(pathUri+"/").build();

        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(publisher);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2, object3).build()));

        DirectoryStream.Filter<? super Path> filter = path -> path.toString().endsWith("key2");

        Iterator<Path> pathIterator =
            provider.newDirectoryStream(Paths.get(URI.create(pathUri+"/")), filter).iterator();

        assertNotNull(pathIterator);
        assertTrue(pathIterator.hasNext());
        assertEquals(fs.getPath(object2.key()), pathIterator.next());
        // object3 and object1 should not be present
        assertFalse(pathIterator.hasNext());
    }

    private int countDirStreamItems(DirectoryStream<Path> stream) {
        AtomicInteger count = new AtomicInteger(0);
        stream.iterator().forEachRemaining(item -> count.incrementAndGet());
        return count.get();
    }

    @Test
    public void createDirectory() throws Exception {
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                PutObjectResponse.builder().build()));

        provider.createDirectory(fs.getPath("/baa/baz/"));

        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockClient, times(1)).putObject(argumentCaptor.capture(), any(AsyncRequestBody.class));
        assertEquals("foo", argumentCaptor.getValue().bucket());
        assertEquals("baa/baz/", argumentCaptor.getValue().key());
    }

    @Test
    public void delete() throws Exception {
        S3Object object1 = S3Object.builder().key("dir/key1").build();
        S3Object object2 = S3Object.builder().key("dir/subdir/key2").build();
        when(mockClient.listObjectsV2(anyConsumer())).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        provider.delete(fs.getPath("/dir"));

        ArgumentCaptor<DeleteObjectsRequest> argumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(argumentCaptor.capture());
        DeleteObjectsRequest captorValue = argumentCaptor.getValue();
        assertEquals("foo", captorValue.bucket());
        List<String> keys = captorValue.delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("dir/key1"));
        assertTrue(keys.contains("dir/subdir/key2"));
    }

    @Test
    public void copy() throws Exception {
        S3Object object1 = S3Object.builder().key("dir1/key1").build();
        S3Object object2 = S3Object.builder().key("dir1/subdir/key2").build();
        when(mockClient.listObjectsV2(anyConsumer())).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        HeadObjectRequest headObjectRequest1 = HeadObjectRequest.builder().bucket("foo").key("dir2/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));

        Path dir1 = fs.getPath("/dir1");
        Path dir2 = fs.getPath("/dir2");
        assertThrows(FileAlreadyExistsException.class, () -> provider.copy(dir1, dir2));
        provider.copy(dir1, dir2, StandardCopyOption.REPLACE_EXISTING);

        ArgumentCaptor<CopyObjectRequest> argumentCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockClient, times(2)).copyObject(argumentCaptor.capture());
        List<CopyObjectRequest> requestValues = argumentCaptor.getAllValues();
        assertEquals("foo", requestValues.get(0).sourceBucket());
        assertEquals("dir1/key1", requestValues.get(0).sourceKey());
        assertEquals("foo", requestValues.get(0).destinationBucket());
        assertEquals("dir2/key1", requestValues.get(0).destinationKey());
        assertEquals("foo", requestValues.get(1).sourceBucket());
        assertEquals("dir1/subdir/key2", requestValues.get(1).sourceKey());
        assertEquals("foo", requestValues.get(1).destinationBucket());
        assertEquals("dir2/subdir/key2", requestValues.get(1).destinationKey());
    }

    @Test
    public void move() throws Exception {
        S3Object object1 = S3Object.builder().key("dir1/key1").build();
        S3Object object2 = S3Object.builder().key("dir1/subdir/key2").build();
        when(mockClient.listObjectsV2(anyConsumer())).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        HeadObjectRequest headObjectRequest1 = HeadObjectRequest.builder().bucket("foo").key("dir2/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        Path dir1 = fs.getPath("/dir1");
        Path dir2 = fs.getPath("/dir2");
        assertThrows(FileAlreadyExistsException.class, () -> provider.move(dir1, dir2));
        provider.move(dir1, dir2, StandardCopyOption.REPLACE_EXISTING);

        ArgumentCaptor<CopyObjectRequest> argumentCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockClient, times(2)).copyObject(argumentCaptor.capture());
        List<CopyObjectRequest> requestValues = argumentCaptor.getAllValues();
        assertEquals("foo", requestValues.get(0).sourceBucket());
        assertEquals("dir1/key1", requestValues.get(0).sourceKey());
        assertEquals("foo", requestValues.get(0).destinationBucket());
        assertEquals("dir2/key1", requestValues.get(0).destinationKey());
        assertEquals("foo", requestValues.get(1).sourceBucket());
        assertEquals("dir1/subdir/key2", requestValues.get(1).sourceKey());
        assertEquals("foo", requestValues.get(1).destinationBucket());
        assertEquals("dir2/subdir/key2", requestValues.get(1).destinationKey());
        ArgumentCaptor<DeleteObjectsRequest> deleteArgumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(deleteArgumentCaptor.capture());
        List<String> keys = deleteArgumentCaptor.getValue().delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("dir1/key1"));
        assertTrue(keys.contains("dir1/subdir/key2"));
    }

    @Test
    public void isSameFile() throws Exception {
        Path foo = fs.getPath("/foo");
        Path baa = fs.getPath("/baa");

        assertFalse(provider.isSameFile(foo, baa));
        assertTrue(provider.isSameFile(foo, foo));

        Path alsoFoo = fs.getPath("foo");
        assertTrue(provider.isSameFile(foo, alsoFoo));

        Path alsoFoo2 = fs.getPath("./foo");
        assertTrue(provider.isSameFile(foo, alsoFoo2));
    }

    @Test
    public void isHidden() {
        Path foo = fs.getPath("/foo");
        //s3 doesn't have hidden files
        Path baa = fs.getPath(".baa");

        assertFalse(provider.isHidden(foo));
        assertFalse(provider.isHidden(baa));
    }

    @Test
    public void getFileStore() {
        Path foo = fs.getPath("/foo");
        //s3 doesn't have file stores
        assertNull(provider.getFileStore(foo));
    }

    @Test
    public void checkAccessWithoutException() throws Exception {

        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));

        Path foo = fs.getPath("/foo");
        provider.checkAccess(foo, AccessMode.READ);
        provider.checkAccess(foo, AccessMode.EXECUTE);
        provider.checkAccess(foo);
    }

    @Test
    public void checkAccessWhenAccessDenied() {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(403).build())
                        .build()));

        Path foo = fs.getPath("/foo");
        assertThrows(AccessDeniedException.class, () -> provider.checkAccess(foo));
    }

    @Test
    public void checkAccessWhenNoSuchFile() {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build())
                        .build()));

        Path foo = fs.getPath("/foo");
        assertThrows(NoSuchFileException.class, () -> provider.checkAccess(foo));
    }

    @Test
    public void checkWriteAccess() throws Exception {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));
        provider.checkAccess(fs.getPath("foo"), AccessMode.WRITE);
    }

    @Test
    public void getFileAttributeView() {
        Path foo = fs.getPath("/foo");
        final BasicFileAttributeView fileAttributeView = provider.getFileAttributeView(foo, BasicFileAttributeView.class);
        assertNotNull(fileAttributeView);
        assertTrue(fileAttributeView instanceof S3BasicFileAttributeView);
    }

    @Test
    public void getFileAttributeViewIllegalArg() {
        Path foo = fs.getPath("/foo");
        assertThrows(IllegalArgumentException.class, () -> provider.getFileAttributeView(foo, FileAttributeView.class));
    }

    @Test
    public void readAttributes() {
        Path foo = fs.getPath("/foo");
        final BasicFileAttributes basicFileAttributes = provider.readAttributes(foo, BasicFileAttributes.class);
        assertNotNull(basicFileAttributes);
        assertTrue(basicFileAttributes instanceof S3BasicFileAttributes);
    }

    @Test
    public void testReadAttributes() {
        Path foo = fs.getPath("/foo");
        Path fooDir = fs.getPath("/foo/");

        when(mockClient.headObject(anyConsumer())).thenReturn(CompletableFuture.completedFuture(
                HeadObjectResponse.builder()
                        .lastModified(Instant.EPOCH)
                        .contentLength(100L)
                        .eTag("abcdef")
                        .build()));

        Map<String, Object> attributes = provider.readAttributes(foo, "*");
        assertTrue(attributes.size() >= 9);

        attributes = provider.readAttributes(foo, "lastModifiedTime,size,fileKey");
        assertEquals(3, attributes.size());
        assertEquals(FileTime.from(Instant.EPOCH), attributes.get("lastModifiedTime"));
        assertEquals(100L, attributes.get("size"));

        assertEquals(Collections.emptyMap(), provider.readAttributes(fooDir, "*"));
    }

    @Test
    public void setAttribute() {
        Path foo = fs.getPath("/foo");
        assertThrows(UnsupportedOperationException.class, () -> provider.setAttribute(foo, "x", "y"));
    }
    
        
    @Test
    public void defaultForcePathStyle() throws Exception {
        final FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();
        
        fs.clientProvider().asyncClientBuilder(BUILDER);
        fs.client(); fs.close();

        assertNull(BUILDER.forcePathStyle);
    }

}
