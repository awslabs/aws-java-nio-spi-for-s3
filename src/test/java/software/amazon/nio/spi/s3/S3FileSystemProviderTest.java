/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

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
        fs = (S3FileSystem) provider.getPath(URI.create(pathUri)).getFileSystem();
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
    @DisplayName("newFileSystem(Path, env) should throw")
    public void newFileSystemPath() {
        assertThatThrownBy(
            () -> new S3FileSystemProvider().newFileSystem(Paths.get(pathUri), Collections.emptyMap())
        ).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void newFileSystemUri() {
        final var uri = URI.create(pathUri);
        final var env = Collections.<String, Object>emptyMap();

        assertThatCode(() -> provider.newFileSystem(uri, null))
                .doesNotThrowAnyExceptionExcept(FileSystemAlreadyExistsException.class, IOException.class);

        assertThatCode(() -> provider.newFileSystem(uri, env))
                .doesNotThrowAnyExceptionExcept(FileSystemAlreadyExistsException.class, IOException.class);

        // Exercise the real config-merge + bucket-creation path with a mixed env map that combines
        // config keys (merged into the configuration) and bucket-creation keys (consumed directly).
        final var mixedEnv = Map.<String, Object>of(
            S3NioSpiConfiguration.AWS_REGION_PROPERTY, "us-east-1",
            S3NioSpiConfiguration.S3_SPI_TIMEOUT_LOW_PROPERTY, "2",
            "acl", "private",
            "grantRead", "id=abc",
            "locationConstraint", "us-east-1");
        assertThatCode(() -> provider.newFileSystem(URI.create("s3://another-bucket"), mixedEnv))
                .doesNotThrowAnyExceptionExcept(FileSystemAlreadyExistsException.class, IOException.class);
    }

    @Test
    @DisplayName("newFileSystem should add the filesystem to the cache so getFileSystem returns the same instance")
    public void newFileSystemShouldCacheFileSystem() throws IOException {
        // Use a fresh bucket name that isn't already in the cache
        final var newBucketUri = URI.create("s3://new-cached-bucket/key");

        // Create a provider that bypasses the actual S3 createBucket call
        var testProvider = new S3FileSystemProvider() {
            @Override
            public FileSystem newFileSystem(final URI uri, final Map<String, ?> env) throws IOException {
                if (!uri.getScheme().equals(getScheme())) {
                    throw new IllegalArgumentException("URI scheme must be " + getScheme());
                }
                var info = fileSystemInfo(uri);
                var config = new S3NioSpiConfiguration().withEndpoint(info.endpoint()).withBucketName(info.bucket());
                if (info.accessKey() != null) {
                    config.withCredentials(info.accessKey(), info.accessSecret());
                }
                // Skip the actual bucket creation and just cache the filesystem
                return getOrCreateFileSystem(info.key(), config);
            }
        };

        // The filesystem should not be in the cache before newFileSystem is called
        assertFalse(testProvider.getFsCache().containsKey("new-cached-bucket"));

        // Call newFileSystem
        var createdFs = testProvider.newFileSystem(newBucketUri, null);

        // The filesystem should now be in the cache
        assertTrue(testProvider.getFsCache().containsKey("new-cached-bucket"));

        // getFileSystem should return the same instance
        var retrievedFs = testProvider.getFileSystem(newBucketUri);
        assertSame(createdFs, retrievedFs);

        // Clean up
        testProvider.closeFileSystem(createdFs);
    }

    @Test
    @DisplayName("newFileSystem merges the env map (minus bucket-creation keys) into the configuration")
    public void newFileSystemMergesEnvIntoConfig() throws IOException {
        final var uri = URI.create("s3://env-merged-bucket");
        final var credentials = AwsBasicCredentials.create("k", "s");

        // Subclass skips the remote createBucket call but reproduces the real config-merge path.
        var testProvider = new S3FileSystemProvider() {
            @Override
            public FileSystem newFileSystem(final URI uri, final Map<String, ?> env) {
                var info = fileSystemInfo(uri);
                var config = new S3NioSpiConfiguration().withEndpoint(info.endpoint()).withBucketName(info.bucket());
                config.withOverrides(env);
                return getOrCreateFileSystem(info.key(), config);
            }
        };

        var env = Map.of(
            S3NioSpiConfiguration.AWS_REGION_PROPERTY, "eu-central-1",
            S3NioSpiConfiguration.S3_SPI_TIMEOUT_LOW_PROPERTY, "7",
            S3NioSpiConfiguration.S3_SPI_CREDENTIALS_PROPERTY, credentials);

        var fs = (S3FileSystem) testProvider.newFileSystem(uri, env);
        try {
            then(fs.getConfiguration().getRegion()).isEqualTo("eu-central-1");
            then(fs.getConfiguration().getTimeoutLow()).isEqualTo(7L);
            then(fs.getConfiguration().getCredentials()).isSameAs(credentials);
        } finally {
            testProvider.closeFileSystem(fs);
        }
    }

    @Test
    @DisplayName("newFileSystem throws FileSystemAlreadyExistsException when a (lazy) view already exists")
    public void newFileSystemThrowsWhenViewAlreadyExists() {
        final var uri = URI.create("s3://already-materialized-bucket/key");
        // Materialize a lazy view via getPath (does not create a bucket, no network).
        provider.getPath(uri);
        try {
            // A subsequent newFileSystem for the same bucket must throw before any remote side effect.
            assertThrows(FileSystemAlreadyExistsException.class,
                () -> provider.newFileSystem(uri, Collections.<String, Object>emptyMap()));
        } finally {
            provider.closeFileSystem((S3FileSystem) provider.getFileSystem(uri));
        }
    }

    @Test
    @DisplayName("provider operations use the per-filesystem timeout, not a shared provider config (#597)")
    public void perFilesystemTimeoutsUsedByProviderOps() {
        // Two file systems for different buckets with different timeout-low values.
        var fsA = (S3FileSystem) provider.getPath(URI.create("s3://bucket-timeout-a")).getFileSystem();
        var fsB = (S3FileSystem) provider.getPath(URI.create("s3://bucket-timeout-b")).getFileSystem();
        try {
            fsA.getConfiguration().withTimeoutLow(11L);
            fsB.getConfiguration().withTimeoutLow(22L);

            // Each file system reports its own timeout; creating one after the other does not
            // clobber the earlier one (the pre-3.0 shared-provider-config bug).
            then(fsA.getConfiguration().getTimeoutLow()).isEqualTo(11L);
            then(fsB.getConfiguration().getTimeoutLow()).isEqualTo(22L);
        } finally {
            provider.closeFileSystem(fsA);
            provider.closeFileSystem(fsB);
        }
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
        // A filesystem view for pathUri was materialized by getPath in init(), so getFileSystem
        // returns that same instance.
        //
        assertSame(fs, provider.getFileSystem(URI.create(pathUri)));

        //
        // getFileSystem throws FileSystemNotFoundException for a bucket that has never had a file
        // system created (neither via newFileSystem nor via getPath/Paths.get).
        //
        assertThatCode(() -> provider.getFileSystem(URI.create("s3://never-created")))
                .isInstanceOf(FileSystemNotFoundException.class);

        //
        // Once a path is materialized via getPath, getFileSystem returns the same cached instance.
        //
        var cfs = provider.getPath(URI.create("s3://foo2/baa")).getFileSystem();
        var gfs = provider.getFileSystem(URI.create("s3://foo2"));
        assertNotSame(fs, gfs); assertSame(cfs, gfs);
        gfs = provider.getFileSystem(URI.create("s3://foo2/other"));
        assertNotSame(fs, gfs); assertSame(cfs, gfs);
        provider.closeFileSystem((S3FileSystem) cfs);

        // After close the file system is gone again.
        assertThatCode(() -> provider.getFileSystem(URI.create("s3://foo2")))
                .isInstanceOf(FileSystemNotFoundException.class);
    }

    @Test
    public void closingFileSystemDiscardsItFromCache() {
        provider.closeFileSystem(fs);
        assertFalse(provider.getFsCache().containsKey(pathUri));
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

        var object1 = S3Object.builder().key("baa/key1").build();
        var object2 = S3Object.builder().key("baa/key2").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            completedFuture(ListObjectsV2Response.builder().contents(object1, object2).build())
        );

        final var stream = provider.newDirectoryStream(fs.getPath(pathUri+"/"), entry -> true);
        assertThat(stream).hasSize(2);
    }

    @Test
    public void newDirectoryStreamS3AccessDeniedException() {
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenThrow(
                // this is what is thrown by the paginator in the case that access is denied
                new RuntimeException(new ExecutionException(
                        S3Exception.builder()
                                .statusCode(403)
                                .message("AccessDenied")
                                .build()
                ))
        );

        assertThatThrownBy(() -> provider.newDirectoryStream(fs.getPath(pathUri+"/"), entry -> true))
            .isInstanceOf(AccessDeniedException.class)
            .hasMessage("Access to bucket 'foo' denied -> /baa/: AccessDenied");
    }

    @Test
    public void newDirectoryStreamS3BucketNotFoundException() {
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenThrow(
            // this is what is thrown by the paginator in the case that a bucket doesn't exist
            new RuntimeException(new ExecutionException(
                    NoSuchBucketException.builder()
                            .statusCode(404)
                            .message("NoSuchBucket")
                            .build()
            ))
        );

        assertThatThrownBy(() -> provider.newDirectoryStream(fs.getPath(pathUri+"/"), entry -> true))
            .isInstanceOf(FileSystemNotFoundException.class)
            .hasMessage("Bucket 'foo' not found: NoSuchBucket");
    }

    @Test
    public void newDirectoryStreamOtherExceptionsBecomeIOExceptions() {
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenThrow(
                new RuntimeException(new ExecutionException(
                        S3Exception.builder()
                                .statusCode(500)
                                .message("software.amazon.awssdk.services.s3.model.S3Exception: InternalError")
                                .build()
                ))
        );

        assertThatThrownBy(() -> provider.newDirectoryStream(fs.getPath(pathUri+"/"), entry -> true))
            .isInstanceOf(IOException.class)
            .hasMessage("software.amazon.awssdk.services.s3.model.S3Exception: InternalError");
    }

    @Test
    public void newDirectoryStreamFiltersSelf() throws IOException {
        final var publisher = new ListObjectsV2Publisher(mockClient, ListObjectsV2Request.builder().build());
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(publisher);

        var object1 = S3Object.builder().key("baa/key1").build();
        var object2 = S3Object.builder().key("baa/key2").build();
        var object3 = S3Object.builder().key("baa/").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            completedFuture(ListObjectsV2Response.builder().contents(object1, object2, object3).build())
        );

        final var expectedItems = Stream.of(object1, object2)
            .map(obj -> fs.getPath("/" + obj.key()))
            .collect(Collectors.toList());
        try(var stream = provider.newDirectoryStream(fs.getPath(pathUri + "/"), path -> true)){
            assertThat(stream.iterator()).toIterable().containsExactlyElementsOf(expectedItems);
        }
    }

    @Test
    public void newDirectoryStreamFilters() throws IOException {
        final var publisher = new ListObjectsV2Publisher(mockClient, ListObjectsV2Request.builder().build());
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(publisher);

        var object1 = S3Object.builder().key("baa/key1").build();
        var object2 = S3Object.builder().key("baa/key2").build();
        var object3 = S3Object.builder().key("baa/").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            completedFuture(ListObjectsV2Response.builder().contents(object1, object2, object3).build())
        );

        DirectoryStream.Filter<? super Path> filter = path -> path.toString().endsWith("key2");

        try(var stream = provider.newDirectoryStream(fs.getPath(pathUri + "/"), filter)){
            assertThat(stream.iterator()).toIterable().containsExactly(fs.getPath("/" + object2.key()));
        }
    }

    @Test
    @DisplayName("newDirectoryStream should return absolute paths that can be relativized against the parent")
    public void newDirectoryStreamReturnsAbsolutePaths() throws IOException {
        final var publisher = new ListObjectsV2Publisher(mockClient, ListObjectsV2Request.builder().build());
        when(mockClient.listObjectsV2Paginator(anyConsumer())).thenReturn(publisher);

        var object1 = S3Object.builder().key("baa/key1").build();
        var object2 = S3Object.builder().key("baa/subdir/").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            completedFuture(ListObjectsV2Response.builder().contents(object1).commonPrefixes(
                software.amazon.awssdk.services.s3.model.CommonPrefix.builder().prefix("baa/subdir/").build()
            ).build())
        );

        var dir = fs.getPath(pathUri + "/");
        try (var stream = provider.newDirectoryStream(dir, path -> true)) {
            for (Path p : stream) {
                // All returned paths should be absolute
                assertTrue(p.isAbsolute(), "Directory stream entries should be absolute paths");
                // Should be able to relativize without throwing
                assertThatCode(() -> dir.relativize(p)).doesNotThrowAnyException();
            }
        }
    }

    @Test
    public void createDirectory() throws Exception {
        // createDirectory first checks that nothing already exists at/under the prefix.
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().isTruncated(false).build()));
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                PutObjectResponse.builder().build()));

        provider.createDirectory(fs.getPath("/baa/baz/"));

        var argumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockClient, times(1)).putObject(argumentCaptor.capture(), any(AsyncRequestBody.class));
        assertEquals("foo", argumentCaptor.getValue().bucket());
        assertEquals("baa/baz/", argumentCaptor.getValue().key());
    }

    @Test
    public void createDirectory_whenAlreadyExists_shouldThrow() {
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(S3Object.builder().key("baa/baz/").build()).isTruncated(false).build()));

        assertThrows(FileAlreadyExistsException.class, () -> provider.createDirectory(fs.getPath("/baa/baz/")));
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
        // A single existing object is deleted (existence is checked via headObject).
        when(mockClient.headObject(any(HeadObjectRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder().contentLength(1L).build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        provider.delete(fs.getPath("/dir/key1"));

        var argumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(argumentCaptor.capture());
        var captorValue = argumentCaptor.getValue();
        assertEquals("foo", captorValue.bucket());
        var keys = captorValue.delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList());
        assertEquals(1, keys.size());
        assertTrue(keys.contains("dir/key1"));
    }

    @Test
    public void delete_emptyDirectory_deletesMarker() throws Exception {
        // Only the directory marker object itself exists -> the directory is empty and is removed.
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(S3Object.builder().key("dir/").build()).isTruncated(false).build()));
        when(mockClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                DeleteObjectsResponse.builder().build()));

        provider.delete(fs.getPath("/dir/"));

        var argumentCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(mockClient, times(1)).deleteObjects(argumentCaptor.capture());
        var keys = argumentCaptor.getValue().delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList());
        assertEquals(List.of("dir/"), keys);
    }

    @Test
    public void delete_nonEmptyDirectory_shouldThrowDirectoryNotEmpty() {
        var object1 = S3Object.builder().key("dir/key1").build();
        var object2 = S3Object.builder().key("dir/subdir/key2").build();
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                ListObjectsV2Response.builder().contents(object1, object2).isTruncated(false).build()));

        assertThrows(DirectoryNotEmptyException.class, () -> provider.delete(fs.getPath("/dir/")));
        verify(mockClient, never()).deleteObjects(any(DeleteObjectsRequest.class));
    }

    @Test
    public void delete_missingFile_shouldThrowNoSuchFile() {
        when(mockClient.headObject(any(HeadObjectRequest.class))).thenReturn(CompletableFuture.failedFuture(
                NoSuchKeyException.builder().build()));

        assertThrows(NoSuchFileException.class, () -> provider.delete(fs.getPath("/dir/missing")));
        verify(mockClient, never()).deleteObjects(any(DeleteObjectsRequest.class));
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

        var dir1 = fs.getPath("/dir1/");
        var dir2 = fs.getPath("/dir2/");
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

        var dir1 = fs.getPath("/dir1/");
        var dir2 = fs.getPath("/dir2/");
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
        var store = provider.getFileStore(foo);
        assertNotNull(store);
        assertEquals("foo", store.name());
        assertEquals("s3", store.type());
        assertFalse(store.isReadOnly());
    }

    @Test
    public void checkAccessWithoutException() throws Exception {

        when(mockClient.headObject(any(Consumer.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                HeadObjectResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                        .build()));

        var foo = fs.getPath("/foo");
        provider.checkAccess(foo, AccessMode.READ);
        provider.checkAccess(foo, AccessMode.WRITE);
        provider.checkAccess(foo);
    }

    @Test
    public void checkAccessExecute_shouldThrowAccessDenied() {
        var foo = fs.getPath("/foo");
        // S3 objects are never executable.
        assertThrows(AccessDeniedException.class, () -> provider.checkAccess(foo, AccessMode.EXECUTE));
    }

    @Test
    public void checkAccessWithExceptionHeadObject() {
        when(mockClient.headObject(anyConsumer())).thenReturn(CompletableFuture.failedFuture(new IOException()));

        var foo = fs.getPath("/foo");
        assertThrows(IOException.class, () -> provider.checkAccess(foo, AccessMode.READ));
    }

    @Test
    public void checkAccessWithExceptionListObjectsV2() {
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
        assertInstanceOf(S3BasicFileAttributeView.class, fileAttributeView);
    }

    @Test
    public void getFileAttributeViewUnsupportedView() {
        var foo = fs.getPath("/foo");
        final var unsupportedView = provider.getFileAttributeView(foo, FileAttributeView.class);
        assertNull(unsupportedView);
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

        // A directory now reports real attributes (isDirectory == true) rather than an empty map.
        var dirAttributes = provider.readAttributes(fooDir, "*");
        assertFalse(dirAttributes.isEmpty());
        assertEquals(Boolean.TRUE, dirAttributes.get("isDirectory"));

        // An empty attribute string still yields an empty map.
        assertEquals(Collections.emptyMap(), provider.readAttributes(foo, ""));
    }

    @Test
    public void setAttribute() {
        var foo = fs.getPath("/foo");
        assertThrows(UnsupportedOperationException.class, () -> provider.setAttribute(foo, "x", "y"));
    }
    
        
    @Test
    public void defaultForcePathStyle() throws Exception {
        // GIVEN
        final var BUILDER = spy(S3AsyncClient.crtBuilder());
        fs.clientProvider().asyncClientBuilder(BUILDER);

        // WHEN
        fs.client();
        fs.close();

        // THEN verify that the force path style is never set and will therefore be the default
        verify(BUILDER, times(0)).forcePathStyle(any());
    }
}
