/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class S3FileSystemProviderTest {

    S3FileSystemProvider provider;
    S3FileSystem fs;
    String pathUri = "s3://foo/baa";
    @Mock
    S3AsyncClient mockClient;

    @Before
    public void init() {
        provider = new S3FileSystemProvider();
        fs = new S3FileSystem("s3://mybucket", provider);
        when(mockClient.headObject(any(Consumer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
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
        final SeekableByteChannel channel = provider.newByteChannel(mockClient, Paths.get(URI.create(pathUri)), Collections.emptySet());
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

    private int countDirStreamItems(DirectoryStream<Path> stream){
        AtomicInteger count = new AtomicInteger(0);
        stream.iterator().forEachRemaining(item -> count.incrementAndGet());
        return count.get();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createDirectory() {
        provider.createDirectory(fs.getPath("/foo/baa/baz/"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void delete() {
        provider.delete(fs.getPath("/foo"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void copy() {
        S3Path foo = fs.getPath("/foo");
        S3Path baa = fs.getPath("/baa");
        provider.copy(foo, baa);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void move() {
        S3Path foo = fs.getPath("/foo");
        S3Path baa = fs.getPath("/baa");
        provider.move(foo, baa);
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

    @Test(expected = AccessDeniedException.class)
    public void checkAccessWhenAccessDenied() throws IOException, ExecutionException, InterruptedException {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(403).build())
                        .build()));

        S3Path foo = fs.getPath("/foo");
        provider.checkAccess(mockClient, foo);
    }

    @Test(expected = NoSuchFileException.class)
    public void checkAccessWhenNoSuchFile() throws IOException, ExecutionException, InterruptedException {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build())
                        .build()));

        S3Path foo = fs.getPath("/foo");
        provider.checkAccess(mockClient, foo);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void checkWriteAccess() throws IOException {
        provider.checkAccess(fs.getPath("foo"), AccessMode.WRITE);
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

    @Test(expected = IllegalArgumentException.class)
    public void getFileAttributeViewIllegalArg() {
        S3Path foo = fs.getPath("/foo");
        final FileAttributeView fileAttributeView = provider.getFileAttributeView(foo, FileAttributeView.class);
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

    @Test(expected = UnsupportedOperationException.class)
    public void setAttribute() {
        S3Path foo = fs.getPath("/foo");
        provider.setAttribute(foo, "x", "y");
    }
}