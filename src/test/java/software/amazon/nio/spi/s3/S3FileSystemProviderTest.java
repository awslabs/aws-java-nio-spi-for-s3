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
import software.amazon.awssdk.services.s3.S3AsyncClient;
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

import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

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
        var cfs = provider.getFileSystem(URI.create("s3://foo2/baa"), true);
        var gfs = provider.getFileSystem(URI.create("s3://foo2"));
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
            FileSystemNotFoundException.class, () -> provider.getFileSystem(URI.create("s3://nobucket"))
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
        when(mockClient.headObject(anyConsumer())).thenReturn(
            CompletableFuture.completedFuture(
                HeadObjectResponse.builder().lastModified(Instant.now()).contentLength(1L).build()
            )
        );
        final var channel = provider.newByteChannel(Paths.get(URI.create(pathUri)), Collections.singleton(StandardOpenOption.READ));
        assertNotNull(channel);
        assertThat(channel).isInstanceOf(S3SeekableByteChannel.class);
    }

    @Test
    public void newDirectoryStream() throws IOException {
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(
            new ListObjectsV2Publisher(mockClient, ListObjectsV2Request.builder().build())
        );

        var object1 = S3Object.builder().key(pathUri+"/key1").build();
        var object2 = S3Object.builder().key(pathUri+"/key2").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            completedFuture(ListObjectsV2Response.builder().contents(object1, object2).build())
        );

        final var stream = provider.newDirectoryStream(fs.getPath(pathUri+"/"), entry -> true);
        assertThat(stream).hasSize(2);
    }

    @Test
    public void newDirectoryStreamFiltersSelf() throws IOException {
        final var publisher = new ListObjectsV2Publisher(mockClient, ListObjectsV2Request.builder().build());
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(publisher);

        var object1 = S3Object.builder().key(pathUri+"/key1").build();
        var object2 = S3Object.builder().key(pathUri+"/key2").build();
        var object3 = S3Object.builder().key(pathUri+"/").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            completedFuture(ListObjectsV2Response.builder().contents(object1, object2, object3).build())
        );

        final var expectedItems = Stream.of(object1, object2).map(obj -> fs.getPath(obj.key())).collect(Collectors.toList());
        try(var stream = provider.newDirectoryStream(fs.getPath(pathUri + "/"), path -> true)){
            assertThat(stream.iterator()).toIterable().containsExactlyElementsOf(expectedItems);
        }
    }

    @Test
    public void newDirectoryStreamFilters() throws IOException {
        final var publisher = new ListObjectsV2Publisher(mockClient, ListObjectsV2Request.builder().build());
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(publisher);

        var object1 = S3Object.builder().key(pathUri+"/key1").build();
        var object2 = S3Object.builder().key(pathUri+"/key2").build();
        var object3 = S3Object.builder().key(pathUri+"/").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            completedFuture(ListObjectsV2Response.builder().contents(object1, object2, object3).build())
        );

        DirectoryStream.Filter<? super Path> filter = path -> path.toString().endsWith("key2");

        try(var stream = provider.newDirectoryStream(fs.getPath(pathUri + "/"), filter)){
            assertThat(stream.iterator()).toIterable().containsExactly(fs.getPath(object2.key()));
        }
    }

    private int countDirStreamItems(DirectoryStream<Path> stream) {
        var count = new AtomicInteger(0);
        stream.iterator().forEachRemaining(item -> count.incrementAndGet());
        return count.get();
    }

    @Test
    public void createDirectory() throws Exception {
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                PutObjectResponse.builder().build()));

        provider.createDirectory(fs.getPath("/baa/baz/"));

        var argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockClient, times(1)).putObject(argumentCaptor.capture(), any(AsyncRequestBody.class));
        assertEquals("foo", argumentCaptor.getValue().bucket());
        assertEquals("baa/baz/", argumentCaptor.getValue().key());
    }

    @Test
    public void createRootDirectory_shouldFail() {
        assertThatThrownBy(() -> provider.createDirectory(fs.getPath("/")))
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessage("Root directory already exists");
    }

    @Test
    public void createRooDirectory_withEmpty_shouldFail(){
        assertThatThrownBy(() -> provider.createDirectory(fs.getPath("")))
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessage("Root directory already exists");
    }

    @Test
    public void delete() throws Exception {
        var object1 = S3Object.builder().key("dir/key1").build();
        var object2 = S3Object.builder().key("dir/subdir/key2").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        provider.delete(fs.getPath("/dir"));

        var argumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(argumentCaptor.capture());
        var captorValue = argumentCaptor.getValue();
        assertEquals("foo", captorValue.bucket());
        var keys = captorValue.delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("dir/key1"));
        assertTrue(keys.contains("dir/subdir/key2"));
    }

    @Test
    public void copy() throws Exception {
        var object1 = S3Object.builder().key("dir1/key1").build();
        var object2 = S3Object.builder().key("dir1/subdir/key2").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        var headObjectRequest1 = HeadObjectRequest.builder().bucket("foo").key("dir2/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));

        var dir1 = fs.getPath("/dir1");
        var dir2 = fs.getPath("/dir2");
        assertThrows(FileAlreadyExistsException.class, () -> provider.copy(dir1, dir2));
        provider.copy(dir1, dir2, StandardCopyOption.REPLACE_EXISTING);

        var argumentCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockClient, times(2)).copyObject(argumentCaptor.capture());
        var requestValues = argumentCaptor.getAllValues();
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
        var object1 = S3Object.builder().key("dir1/key1").build();
        var object2 = S3Object.builder().key("dir1/subdir/key2").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        var headObjectRequest1 = HeadObjectRequest.builder().bucket("foo").key("dir2/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        var dir1 = fs.getPath("/dir1");
        var dir2 = fs.getPath("/dir2");
        assertThrows(FileAlreadyExistsException.class, () -> provider.move(dir1, dir2));
        provider.move(dir1, dir2, StandardCopyOption.REPLACE_EXISTING);

        var argumentCaptor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(mockClient, times(2)).copyObject(argumentCaptor.capture());
        var requestValues = argumentCaptor.getAllValues();
        assertEquals("foo", requestValues.get(0).sourceBucket());
        assertEquals("dir1/key1", requestValues.get(0).sourceKey());
        assertEquals("foo", requestValues.get(0).destinationBucket());
        assertEquals("dir2/key1", requestValues.get(0).destinationKey());
        assertEquals("foo", requestValues.get(1).sourceBucket());
        assertEquals("dir1/subdir/key2", requestValues.get(1).sourceKey());
        assertEquals("foo", requestValues.get(1).destinationBucket());
        assertEquals("dir2/subdir/key2", requestValues.get(1).destinationKey());
        var deleteArgumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(deleteArgumentCaptor.capture());
        var keys = deleteArgumentCaptor.getValue().delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("dir1/key1"));
        assertTrue(keys.contains("dir1/subdir/key2"));
    }

    @Test
    public void isSameFile() throws Exception {
        var foo = fs.getPath("/foo");
        var baa = fs.getPath("/baa");

        assertFalse(provider.isSameFile(foo, baa));
        assertTrue(provider.isSameFile(foo, foo));

        var alsoFoo = fs.getPath("foo");
        assertTrue(provider.isSameFile(foo, alsoFoo));

        var alsoFoo2 = fs.getPath("./foo");
        assertTrue(provider.isSameFile(foo, alsoFoo2));
    }

    @Test
    public void isHidden() {
        var foo = fs.getPath("/foo");
        //s3 doesn't have hidden files
        var baa = fs.getPath(".baa");

        assertFalse(provider.isHidden(foo));
        assertFalse(provider.isHidden(baa));
    }

    @Test
    public void getFileStore() {
        var foo = fs.getPath("/foo");
        //s3 doesn't have file stores
        assertNull(provider.getFileStore(foo));
    }

    @Test
    public void checkAccessWithoutException() throws Exception {

        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));

        var foo = fs.getPath("/foo");
        provider.checkAccess(foo, AccessMode.READ);
        provider.checkAccess(foo, AccessMode.EXECUTE);
        provider.checkAccess(foo);
    }

    @Test
    public void checkAccessWithExceptionHeadObject() throws Exception {
        when(mockClient.headObject(anyConsumer())).thenReturn(CompletableFuture.failedFuture(new IOException()));

        var foo = fs.getPath("/foo");
        assertThrows(IOException.class, () -> provider.checkAccess(foo, AccessMode.READ));
    }

    @Test
    public void checkAccessWithExceptionListObjectsV2() throws Exception {
        when(mockClient.listObjectsV2(anyConsumer())).thenReturn(CompletableFuture.failedFuture(new IOException()));

        var foo = fs.getPath("/dir/");
        assertThrows(IOException.class, () -> provider.checkAccess(foo, AccessMode.READ));
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
        var foo = fs.getPath("/foo");
        final var fileAttributeView = provider.getFileAttributeView(foo, BasicFileAttributeView.class);
        assertNotNull(fileAttributeView);
        assertTrue(fileAttributeView instanceof S3BasicFileAttributeView);
    }

    @Test
    public void getFileAttributeViewIllegalArg() {
        var foo = fs.getPath("/foo");
        assertThrows(IllegalArgumentException.class, () -> provider.getFileAttributeView(foo, FileAttributeView.class));
    }

    @Test
    public void readAttributes() throws IOException {
        var foo = fs.getPath("/foo");
        when(mockClient.headObject(anyConsumer())).thenReturn(completedFuture(
            HeadObjectResponse.builder()
                .lastModified(Instant.EPOCH)
                .contentLength(100L)
                .eTag("abcdef")
                .build()));
        final var basicFileAttributes = provider.readAttributes(foo, BasicFileAttributes.class);
        assertNotNull(basicFileAttributes);
        assertThat(basicFileAttributes).isInstanceOf(S3BasicFileAttributes.class);
    }

    @Test
    public void testReadAttributes() throws IOException {
        var foo = fs.getPath("/foo");
        var fooDir = fs.getPath("/foo/");

        when(mockClient.headObject(anyConsumer())).thenReturn(completedFuture(
                HeadObjectResponse.builder()
                        .lastModified(Instant.EPOCH)
                        .contentLength(100L)
                        .eTag("abcdef")
                        .build()));

        var attributes = provider.readAttributes(foo, "*");
        assertTrue(attributes.size() >= 9);

        attributes = provider.readAttributes(foo, "lastModifiedTime,size,fileKey");
        assertEquals(3, attributes.size());
        assertEquals(FileTime.from(Instant.EPOCH), attributes.get("lastModifiedTime"));
        assertEquals(100L, attributes.get("size"));

        assertEquals(Collections.emptyMap(), provider.readAttributes(fooDir, "*"));
    }

    @Test
    public void setAttribute() {
        var foo = fs.getPath("/foo");
        assertThrows(UnsupportedOperationException.class, () -> provider.setAttribute(foo, "x", "y"));
    }
    
        
    @Test
    public void defaultForcePathStyle() throws Exception {
        final var BUILDER = new FakeAsyncS3ClientBuilder();
        
        fs.clientProvider().asyncClientBuilder(BUILDER);
        fs.client(); fs.close();

        assertNull(BUILDER.forcePathStyle);
    }
}
