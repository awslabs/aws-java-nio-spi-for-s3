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
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.After;

import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class S3FileSystemProviderTest {

    S3FileSystemProvider provider;
    S3FileSystem fileSystem;
    String pathUri = "s3://foo/baa";
    @Mock
    S3AsyncClient mockClient;


    //
    // TODO: switch to JUnit5 and system lambda
    //
    @Rule
    public final ProvideSystemProperty AWS_ACCESS_KEY
	 = new ProvideSystemProperty("aws.accessKeyId", "akey");
    @Rule
    public final ProvideSystemProperty AWS_SECRET_ACCESS_KEY
	 = new ProvideSystemProperty("aws.secretAccessKey", "asecret");

    @Before
    public void init() {
        provider = new S3FileSystemProvider();
        provider.clientProvider = new S3ClientProvider() {
            @Override
            protected S3AsyncClient generateAsyncClient(String bucketName) {
                return mockClient;
            }
        };
        fileSystem = provider.newFileSystem(URI.create(pathUri));
        when(mockClient.headObject(any(Consumer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
    }

    @After
    public void after() {
       provider.closeFileSystem(fileSystem);
    }

    @Test
    public void getScheme() {
        assertEquals("s3", provider.getScheme());
    }

    @Test
    public void newFileSystem() {
        //
        // A filesystem for pathUri has been created already ;)
        //
        try {
            provider.newFileSystem(URI.create(pathUri));
            fail("filesystem created twice!");
        } catch (FileSystemAlreadyExistsException x) {
            assertTrue(x.getMessage().contains("'foo'"));
        }

        //
        // New AWS S3 file system
        //
        S3FileSystem fs = provider.newFileSystem(URI.create("s3://foo2/baa"));
        assertNotNull(fs); assertEquals("foo2", fs.bucketName()); assertNull(fs.credentials());

        //
        // New AWS S3 file system with same bucket but different path
        //
        try {
            provider.newFileSystem(URI.create("s3://foo2/baa2"));
            fail("filesystem created twice!");
        } catch (FileSystemAlreadyExistsException x) {
            assertTrue(x.getMessage().contains("'foo2'"));
        }
        provider.closeFileSystem(fs);

        //
        // New file system with endpoint no credentials
        //
        fs = provider.newFileSystem(URI.create("s3://endpoint.com/foo2/baa2/dir"));
        assertNotNull(fs); assertNull(fs.credentials());
        assertEquals("foo2", fs.bucketName()); assertEquals("endpoint.com", fs.endpoint());
        provider.closeFileSystem(fs);

        //
        // New file system with existing endpoint, different bucket, no credentials
        //
        fs = provider.newFileSystem(URI.create("s3://endpoint.com/foo3/baa2"));
        assertNotNull(fs); assertNull(fs.credentials());
        assertEquals("foo3", fs.bucketName()); assertEquals("endpoint.com", fs.endpoint());

        //
        // New file system with existing endpoint and bucket, no credentials
        //
        try {
            provider.newFileSystem(URI.create("s3://endpoint.com/foo3/dir2"));
            fail("filesystem created twice!");
        } catch (FileSystemAlreadyExistsException x) {
            assertTrue(x.getMessage().contains("'endpoint.com/foo3'"));
        }
        provider.closeFileSystem(fs);

        //
        // New file system with existing endpoint but different port, different bucket, no credentials
        //
        fs = provider.newFileSystem(URI.create("s3://endpoint.com:1234/foo3/baa2"));
        assertNotNull(fs); assertNull(fs.credentials());
        assertEquals("foo3", fs.bucketName()); assertEquals("endpoint.com:1234", fs.endpoint());
        provider.closeFileSystem(fs);

        //
        // New file system with existing endpoint, same bucket, credentials
        //
        fs = provider.newFileSystem(URI.create("s3://akey:asecret@somewhere.com/foo2/baa2"));
        assertNotNull(fs);assertNotNull(fs.credentials());
        assertEquals("foo2", fs.bucketName()); assertEquals("somewhere.com", fs.endpoint());

        //
        // New file system with same endpoint, same bucket, different credentials
        //
        try {
            fs = provider.newFileSystem(URI.create("s3://anotherkey:anothersecret@somewhere.com/foo2/baa2"));
            fail("filesystem created twice!");
        } catch (FileSystemAlreadyExistsException x) {
            assertTrue(x.getMessage().contains("'somewhere.com/foo2'"));
        }
        provider.closeFileSystem(fs);
    }

    @Test
    public void newFileSystemWrongArguments() {
        //
        // IllegalArgumentException if URI is not good
        //
        try {
            provider.newFileSystem((URI)null);
            fail("mising argument check!");
        } catch (IllegalArgumentException x) {
            assertEquals("uri can not be null", x.getMessage());
        }

        try {
            provider.newFileSystem(URI.create("noscheme"));
            fail("mising argument check!");
        } catch (IllegalArgumentException x) {
            assertEquals(
                "invalid uri 'noscheme', please provide an uri as s3://[key:secret@][host[:port]]/bucket",
                x.getMessage()
            );
        }

        try {
            provider.newFileSystem(URI.create("s3:///"));
            fail("mising argument check!");
        } catch (IllegalArgumentException x) {
            assertEquals(
                "invalid uri 's3:///', please provide an uri as s3://[key:secret@][host[:port]]/bucket",
                x.getMessage()
            );
        }
    }

    @Test
    public void getFileSystem() {
        //
        // A filesystem for pathUri has been created already ;)
        //
        assertSame(fileSystem, provider.getFileSystem(URI.create(pathUri)));

        //
        // New AWS S3 file system
        //
        S3FileSystem cfs = provider.newFileSystem(URI.create("s3://foo2/baa"));
        FileSystem gfs = provider.getFileSystem(URI.create("s3://foo2"));
        assertNotSame(fileSystem, gfs); assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://foo2"));
        assertNotSame(fileSystem, gfs); assertSame(cfs, gfs);
        provider.closeFileSystem(cfs);

        //
        // New AWS S3 file system with same bucket but different path
        //
        cfs = provider.newFileSystem(URI.create("s3://foo3"));
        gfs = provider.getFileSystem(URI.create("s3://foo3/dir"));
        assertNotSame(fileSystem, gfs); assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://foo3/dir"));
        assertNotSame(fileSystem, gfs); assertSame(cfs, gfs);
        provider.closeFileSystem(cfs);

        //
        // New S3 file system with endpoint
        //
        cfs = provider.newFileSystem(URI.create("s3://endpoint.com/foo3"));
        gfs = provider.getFileSystem(URI.create("s3://endpoint.com/foo3"));
        assertNotSame(fileSystem, gfs); assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://endpoint.com/foo3/dir/subdir"));
        assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://key@endpoint.com/foo3/dir/subdir"));
        assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://key:secret@endpoint.com/foo3"));
        assertSame(cfs, gfs);
        provider.closeFileSystem(cfs);

        // Not existing file system
        final URI[] BUCKETS = new URI[] {
            URI.create("s3://nowhere.com/foo2/"),
            URI.create("s3://nowhere.com/foo2/baa2"),
            URI.create("s3://key@nowhere.com/foo2/baa2"),
            URI.create("s3://key:secret@nowhere.com/foo2/baa2")
        };
        //
        // TODO: turn into JUnit5 assertThrows
        //
        try {
            provider.getFileSystem(URI.create("s3://nowhere.com/foo2/baa2"));
            fail("wrong file system!");
        } catch (FileSystemNotFoundException x) {
            //
            // OK!
            //
        }
    }

    @Test
    public void closingFileSystemDiscardsItFromCache() {
        provider.closeFileSystem(fileSystem);

        //
        // TODO: turn into a JUnit5 assertion
        //
        try {
            provider.getFileSystem(URI.create(pathUri));
            fail("file system still available");
        } catch (FileSystemNotFoundException x) {
            //
            // OK!
            //
        }
    }

    @Test
    public void getPath() {
        assertNotNull(provider.getPath(URI.create(pathUri)));

        //
        // Make sure a file system is created if not already done
        //
        final URI U = URI.create("s3://endpoint.com/bucket");
        assertNotNull(provider.getPath(U));
        provider.closeFileSystem(provider.getFileSystem(U));
    }

    @Test
    public void newByteChannel() throws Exception {
        final SeekableByteChannel channel = provider.newByteChannel(Paths.get(URI.create(pathUri)), Collections.singleton(StandardOpenOption.READ));
        assertNotNull(channel);
        assertTrue(channel instanceof S3SeekableByteChannel);
    }

    @Test
    public void newDirectoryStream() throws Exception {

        S3Object object1 = S3Object.builder().key("key1").build();
        S3Object object2 = S3Object.builder().key("foo/key2").build();

        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).build()));

        final DirectoryStream<Path> stream = provider.newDirectoryStream(Paths.get(URI.create(pathUri)), entry -> true);
        assertNotNull(stream);
        assertEquals(2, countDirStreamItems(stream));

        final DirectoryStream<Path> filteredStream = provider.newDirectoryStream(Paths.get(URI.create(pathUri)),
                entry -> entry.endsWith("key2"));
        assertNotNull(filteredStream);
        assertEquals(1, countDirStreamItems(filteredStream));
    }

    @Test
    public void createDirectory() throws Exception {
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                PutObjectResponse.builder().build()));

        provider.createDirectory(fileSystem.getPath("/baa/baz/"));

        ArgumentCaptor<PutObjectRequest> argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockClient, times(1)).putObject(argumentCaptor.capture(), any(AsyncRequestBody.class));
        assertEquals("foo", argumentCaptor.getValue().bucket());
        assertEquals("baa/baz/", argumentCaptor.getValue().key());
    }

    @Test
    public void delete() throws Exception {
        S3Object object1 = S3Object.builder().key("dir/key1").build();
        S3Object object2 = S3Object.builder().key("dir/subdir/key2").build();
        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        provider.delete(fileSystem.getPath("/dir"));

        ArgumentCaptor<DeleteObjectsRequest> argumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(argumentCaptor.capture());
        DeleteObjectsRequest captorValue = argumentCaptor.getValue();
        assertEquals("foo", captorValue.bucket());
        List<String> keys = captorValue.delete().objects().stream().map(objectIdentifier -> objectIdentifier.key()).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("dir/key1"));
        assertTrue(keys.contains("dir/subdir/key2"));
    }

    @Test
    public void copy() throws Exception {
        S3Object object1 = S3Object.builder().key("dir1/key1").build();
        S3Object object2 = S3Object.builder().key("dir1/subdir/key2").build();
        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        HeadObjectRequest headObjectRequest1 = HeadObjectRequest.builder().bucket("foo").key("dir2/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));

        S3Path dir1 = fileSystem.getPath("/dir1");
        S3Path dir2 = fileSystem.getPath("/dir2");
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
        when(mockClient.listObjectsV2(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).nextContinuationToken(null).build()));
        HeadObjectRequest headObjectRequest1 = HeadObjectRequest.builder().bucket("foo").key("dir2/key1").build();
        when(mockClient.headObject(headObjectRequest1)).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().build()));
        when(mockClient.copyObject(any(CopyObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                CopyObjectResponse.builder().build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        S3Path dir1 = fileSystem.getPath("/dir1");
        S3Path dir2 = fileSystem.getPath("/dir2");
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
        List<String> keys = deleteArgumentCaptor.getValue().delete().objects().stream().map(objectIdentifier -> objectIdentifier.key()).collect(Collectors.toList());
        assertEquals(2, keys.size());
        assertTrue(keys.contains("dir1/key1"));
        assertTrue(keys.contains("dir1/subdir/key2"));
    }

    @Test
    public void isSameFile() throws Exception {
        S3Path foo = fileSystem.getPath("/foo");
        S3Path baa = fileSystem.getPath("/baa");

        assertFalse(provider.isSameFile(foo, baa));
        assertTrue(provider.isSameFile(foo, foo));

        S3Path alsoFoo = fileSystem.getPath("foo");
        assertTrue(provider.isSameFile(foo, alsoFoo));

        S3Path alsoFoo2 = fileSystem.getPath("./foo");
        assertTrue(provider.isSameFile(foo, alsoFoo2));
    }


    @Test
    public void isHidden() {
        S3Path foo = fileSystem.getPath("/foo");
        //s3 doesn't have hidden files
        S3Path baa = fileSystem.getPath(".baa");

        assertFalse(provider.isHidden(foo));
        assertFalse(provider.isHidden(baa));
    }

    @Test
    public void getFileStore() {
        S3Path foo = fileSystem.getPath("/foo");
        //s3 doesn't have file stores
        assertNull(provider.getFileStore(foo));
    }

    @Test
    public void checkAccessWithoutException() throws Exception {

        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));

        S3Path foo = fileSystem.getPath("/foo");
        provider.checkAccess(foo, AccessMode.READ);
        provider.checkAccess(foo, AccessMode.EXECUTE);
        provider.checkAccess(foo);
    }

    @Test(expected = AccessDeniedException.class)
    public void checkAccessWhenAccessDenied() throws Exception {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(403).build())
                        .build()));

        S3Path foo = fileSystem.getPath("/foo");
        provider.checkAccess(foo);
    }

    @Test(expected = NoSuchFileException.class)
    public void checkAccessWhenNoSuchFile() throws Exception {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(404).build())
                        .build()));

        S3Path foo = fileSystem.getPath("/foo");
        provider.checkAccess(foo);
    }

    @Test
    public void checkWriteAccess() throws Exception {
        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));
        provider.checkAccess(fileSystem.getPath("foo"), AccessMode.WRITE);
    }

    @Test
    public void getFileAttributeView() {
        S3Path foo = fileSystem.getPath("/foo");
        final BasicFileAttributeView fileAttributeView = provider.getFileAttributeView(foo, BasicFileAttributeView.class);
        assertNotNull(fileAttributeView);
        assertTrue(fileAttributeView instanceof S3FileAttributeView);

        final S3FileAttributeView fileAttributeView1 = provider.getFileAttributeView(foo, S3FileAttributeView.class);
        assertNotNull(fileAttributeView1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getFileAttributeViewIllegalArg() {
        S3Path foo = fileSystem.getPath("/foo");
        provider.getFileAttributeView(foo, FileAttributeView.class);
    }

    @Test
    public void readAttributes() {
        S3Path foo = fileSystem.getPath("/foo");
        final BasicFileAttributes BasicFileAttributes = provider.readAttributes(foo, BasicFileAttributes.class);
        assertNotNull(BasicFileAttributes);
        assertTrue(BasicFileAttributes instanceof S3BasicFileAttributes);

        final S3BasicFileAttributes s3BasicFileAttributes = provider.readAttributes(foo, S3BasicFileAttributes.class);
        assertNotNull(s3BasicFileAttributes);
    }

    @Test
    public void testReadAttributes() {
        S3Path foo = fileSystem.getPath("/foo");
        S3Path fooDir = fileSystem.getPath("/foo/");

        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.completedFuture(
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

    @Test(expected = UnsupportedOperationException.class)
    public void setAttribute() {
        S3Path foo = fileSystem.getPath("/foo");
        provider.setAttribute(foo, "x", "y");
    }

    @Test
    public void getS3FileSystemFromS3URI() throws Exception {
        final URI U = URI.create("s3://endpoint/bucket1");
        S3FileSystem fs = (S3FileSystem)FileSystems.newFileSystem(U, Collections.EMPTY_MAP);
        assertNotNull(fs);
        try {
            FileSystems.newFileSystem(URI.create("s3://endpoint/bucket1"), Collections.EMPTY_MAP);
        } catch (FileSystemAlreadyExistsException x) {
            assertEquals("a file system already exists for uri 'endpoint', use getFileSystem() instead", x.getMessage());
        }
        assertSame(fs, FileSystems.getFileSystem(U));
        fs.close();
    }

    // --------------------------------------------------------- private methods

    private int countDirStreamItems(DirectoryStream<Path> stream) {
        AtomicInteger count = new AtomicInteger(0);
        stream.iterator().forEachRemaining(item -> count.incrementAndGet());
        return count.get();
    }

}