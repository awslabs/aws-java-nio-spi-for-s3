/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
@DisplayName("S3SeekableByteChannel Streaming Multipart Upload Integration")
public class S3SeekableByteChannelStreamingIntegrationTest {

    /**
     * A marker interface with "Crt" in the name to satisfy the CRT client validation.
     */
    interface S3CrtAsyncClientMarker extends S3AsyncClient {
    }

    S3FileSystem fs;
    S3Path path;

    @Mock
    S3AsyncClient mockClient;

    @BeforeEach
    void init() {
        var provider = new S3FileSystemProvider();
        fs = (S3FileSystem) provider.getFileSystem(URI.create("s3://test-bucket"));
        fs.clientProvider(new FixedS3ClientProvider(mockClient));
        path = (S3Path) fs.getPath("/test-object");
    }

    @AfterEach
    void after() throws IOException {
        fs.close();
    }

    /**
     * Creates a mock CRT client by mocking the marker interface that has "Crt" in its name.
     */
    private S3AsyncClient createMockCrtClient() {
        return mock(S3CrtAsyncClientMarker.class);
    }

    @Nested
    @DisplayName("8.1 - Channel creation with streaming option")
    class ChannelCreationTests {

        @Test
        @DisplayName("Creates S3StreamingMultipartUploadChannel when streaming option is present with CRT client")
        void createsStreamingChannelWithCrtClient() throws IOException {
            // The mock client class name must contain "Crt"
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(CREATE);
            options.add(S3OpenOption.streamingMultipartUpload());

            try (var channel = new S3SeekableByteChannel(path, crtClient, options)) {
                assertThat(channel.getWriteDelegate()).isInstanceOf(S3StreamingMultipartUploadChannel.class);
                assertThat(channel.getReadDelegate()).isNull();
            }
        }

        @Test
        @DisplayName("Throws UnsupportedOperationException when streaming option used without CRT client")
        void throwsWhenNonCrtClient() {
            // mockClient's class name is a Mockito proxy, not containing "Crt"
            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(CREATE);
            options.add(S3OpenOption.streamingMultipartUpload());

            assertThatThrownBy(() -> new S3SeekableByteChannel(path, mockClient, options))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Streaming multipart upload requires the AWS CRT client");
        }

        @Test
        @DisplayName("Throws IllegalArgumentException when streaming option combined with READ")
        void throwsWhenCombinedWithRead() {
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(READ);
            options.add(S3OpenOption.streamingMultipartUpload());

            assertThatThrownBy(() -> new S3SeekableByteChannel(path, crtClient, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("write-only")
                .hasMessageContaining("cannot be combined with READ");
        }

        @Test
        @DisplayName("Creates S3WritableByteChannel when no streaming option is present")
        void createsWritableChannelWithoutStreamingOption() throws IOException {
            // Setup mocks for the S3WritableByteChannel path
            // Use CREATE_NEW + assumeObjectNotExists to skip download
            lenient().when(mockClient.headObject(anyConsumer())).thenCallRealMethod();
            lenient().when(mockClient.headObject(any(HeadObjectRequest.class))).thenReturn(
                CompletableFuture.failedFuture(NoSuchKeyException.builder().build())
            );
            lenient().when(mockClient.putObject(any(software.amazon.awssdk.services.s3.model.PutObjectRequest.class),
                any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    software.amazon.awssdk.services.s3.model.PutObjectResponse.builder().build()));

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(S3OpenOption.assumeObjectNotExists());

            try (var channel = new S3SeekableByteChannel(path, mockClient, options)) {
                assertThat(channel.getWriteDelegate()).isInstanceOf(S3WritableByteChannel.class);
            }
        }
    }

    @Nested
    @DisplayName("8.2 - Option combinations")
    class OptionCombinationTests {

        @Test
        @DisplayName("useTransferManager is overridden by streaming option")
        void streamingOverridesTransferManager() throws IOException {
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(CREATE);
            options.add(S3OpenOption.streamingMultipartUpload());
            options.add(S3OpenOption.useTransferManager());

            try (var channel = new S3SeekableByteChannel(path, crtClient, options)) {
                // Streaming takes precedence over transfer manager
                assertThat(channel.getWriteDelegate()).isInstanceOf(S3StreamingMultipartUploadChannel.class);
            }
        }

        @Test
        @DisplayName("CREATE_NEW with streaming option checks existence and throws if object exists")
        void createNewThrowsIfObjectExists() {
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            // Mock headObject to indicate object exists
            when(crtClient.headObject(any(HeadObjectRequest.class))).thenReturn(
                CompletableFuture.completedFuture(
                    HeadObjectResponse.builder().contentLength(100L).lastModified(Instant.now()).build()
                )
            );
            lenient().when(crtClient.headObject(anyConsumer())).thenCallRealMethod();

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(S3OpenOption.streamingMultipartUpload());

            assertThatThrownBy(() -> new S3SeekableByteChannel(path, crtClient, options))
                .isInstanceOf(FileAlreadyExistsException.class);
        }

        @Test
        @DisplayName("CREATE_NEW with streaming option succeeds if object does not exist")
        void createNewSucceedsIfObjectNotExists() throws IOException {
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            // Mock headObject to indicate object does not exist
            when(crtClient.headObject(any(HeadObjectRequest.class))).thenReturn(
                CompletableFuture.failedFuture(NoSuchKeyException.builder().build())
            );
            lenient().when(crtClient.headObject(anyConsumer())).thenCallRealMethod();

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(StandardOpenOption.CREATE_NEW);
            options.add(S3OpenOption.streamingMultipartUpload());

            try (var channel = new S3SeekableByteChannel(path, crtClient, options)) {
                assertThat(channel.getWriteDelegate()).isInstanceOf(S3StreamingMultipartUploadChannel.class);
            }
        }

        @Test
        @DisplayName("assumeObjectNotExists with streaming option skips download")
        void assumeObjectNotExistsSkipsDownload() throws IOException {
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(CREATE);
            options.add(S3OpenOption.streamingMultipartUpload());
            options.add(S3OpenOption.assumeObjectNotExists());

            try (var channel = new S3SeekableByteChannel(path, crtClient, options)) {
                assertThat(channel.getWriteDelegate()).isInstanceOf(S3StreamingMultipartUploadChannel.class);
                // No getObject call should have been made
                verify(crtClient, never()).getObject(
                    any(software.amazon.awssdk.services.s3.model.GetObjectRequest.class),
                    any(AsyncResponseTransformer.class));
            }
        }
    }

    @Nested
    @DisplayName("8.3 - force() support")
    class ForceTests {

        @Test
        @DisplayName("force() completes current session and allows subsequent writes")
        void forceCompletesSessionAndAllowsSubsequentWrites() throws IOException {
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            // Mock multipart upload lifecycle
            when(crtClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    CreateMultipartUploadResponse.builder().uploadId("upload-1").build()))
                .thenReturn(CompletableFuture.completedFuture(
                    CreateMultipartUploadResponse.builder().uploadId("upload-2").build()));

            when(crtClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    UploadPartResponse.builder().eTag("etag-1").build()));

            when(crtClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    CompleteMultipartUploadResponse.builder().build()));

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(CREATE);
            options.add(S3OpenOption.streamingMultipartUpload());

            var streamingChannel = new S3StreamingMultipartUploadChannel(
                path, crtClient,
                (S3StreamingMultipartUpload) S3OpenOption.streamingMultipartUpload());

            // Write some data
            ByteBuffer data = ByteBuffer.allocate(100);
            data.put(new byte[100]);
            data.flip();
            streamingChannel.write(data);

            assertThat(streamingChannel.getUploadId()).isEqualTo("upload-1");

            // Force completes the session
            streamingChannel.force();

            // Upload ID should be reset
            assertThat(streamingChannel.getUploadId()).isNull();
            assertThat(streamingChannel.getNextPartNumber()).isEqualTo(1);
            assertThat(streamingChannel.getCompletedParts()).isEmpty();

            // Write more data - should start a new session
            ByteBuffer moreData = ByteBuffer.allocate(50);
            moreData.put(new byte[50]);
            moreData.flip();
            streamingChannel.write(moreData);

            assertThat(streamingChannel.getUploadId()).isEqualTo("upload-2");

            // Clean up
            when(crtClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    CompleteMultipartUploadResponse.builder().build()));
            streamingChannel.close();
        }

        @Test
        @DisplayName("force() with no active session is a no-op")
        void forceWithNoSessionIsNoOp() throws IOException {
            var crtClient = createMockCrtClient();

            var streamingChannel = new S3StreamingMultipartUploadChannel(
                path, crtClient,
                (S3StreamingMultipartUpload) S3OpenOption.streamingMultipartUpload());

            // force() without any writes should not throw
            streamingChannel.force();

            // Verify no S3 calls were made
            verify(crtClient, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

            streamingChannel.close();
        }

        @Test
        @DisplayName("force() via S3FileChannel delegates to streaming channel")
        void forceViaFileChannelDelegatesToStreamingChannel() throws IOException {
            var crtClient = createMockCrtClient();
            fs.clientProvider(new FixedS3ClientProvider(crtClient));

            // Mock multipart upload lifecycle
            when(crtClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    CreateMultipartUploadResponse.builder().uploadId("upload-1").build()));

            when(crtClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    UploadPartResponse.builder().eTag("etag-1").build()));

            when(crtClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                    CompleteMultipartUploadResponse.builder().build()));

            Set<OpenOption> options = new HashSet<>();
            options.add(WRITE);
            options.add(CREATE);
            options.add(S3OpenOption.streamingMultipartUpload());

            var seekableChannel = new S3SeekableByteChannel(path, crtClient, options);
            var fileChannel = new S3FileChannel(seekableChannel);

            // Write some data
            ByteBuffer data = ByteBuffer.allocate(100);
            data.put(new byte[100]);
            data.flip();
            fileChannel.write(data);

            // force() should complete the session
            fileChannel.force(true);

            // Verify completeMultipartUpload was called
            verify(crtClient).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

            fileChannel.close();
        }
    }
}
