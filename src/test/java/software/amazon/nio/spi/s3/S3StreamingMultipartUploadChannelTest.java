/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Unit tests for backpressure and concurrency control in S3StreamingMultipartUploadChannel.
 * Verifies:
 * - Writes block when all permits are taken (max in-flight reached)
 * - Failure propagation: when an async upload fails, the next write() or close() throws IOException
 * - Semaphore permits are released on both success and failure
 */
class S3StreamingMultipartUploadChannelTest {

    private static final int PART_SIZE = 5 * 1024 * 1024; // 5 MiB (minimum)

    private S3AsyncClient mockClient;
    private S3Path mockPath;

    @BeforeEach
    void setUp() {
        mockClient = mock(S3AsyncClient.class);
        mockPath = mock(S3Path.class);
        when(mockPath.bucketName()).thenReturn("test-bucket");
        when(mockPath.getKey()).thenReturn("test-key");
        when(mockPath.toUri()).thenReturn(java.net.URI.create("s3://test-bucket/test-key"));

        // Default: createMultipartUpload succeeds
        when(mockClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CreateMultipartUploadResponse.builder().uploadId("test-upload-id").build()));

        // Default: abortMultipartUpload succeeds
        when(mockClient.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                AbortMultipartUploadResponse.builder().build()));
    }

    @Test
    @DisplayName("write blocks when all in-flight permits are taken")
    void writeBlocksWhenAllPermitsAreTaken() throws Exception {
        int maxInFlight = 1;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Use a CompletableFuture that we never complete - this holds the permit
        var hangingFuture = new CompletableFuture<UploadPartResponse>();
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(hangingFuture);

        // First write fills the buffer and triggers an upload (acquires the only permit)
        ByteBuffer firstWrite = ByteBuffer.allocate(PART_SIZE);
        channel.write(firstWrite);

        // Now the permit is taken. The next write that fills the buffer should block.
        var writeBlocked = new AtomicBoolean(false);
        var writeCompleted = new AtomicBoolean(false);
        var blockedLatch = new CountDownLatch(1);

        Thread writerThread = new Thread(() -> {
            try {
                // This write will fill the buffer and try to upload, which requires a permit
                ByteBuffer secondWrite = ByteBuffer.allocate(PART_SIZE);
                writeBlocked.set(true);
                blockedLatch.countDown();
                channel.write(secondWrite);
                writeCompleted.set(true);
            } catch (IOException e) {
                // Expected if channel is closed while blocked
            }
        });

        writerThread.start();

        // Wait for the writer thread to start and attempt the write
        blockedLatch.await(2, TimeUnit.SECONDS);
        // Give the thread time to actually block on the semaphore
        Thread.sleep(200);

        // The write should be blocked (not completed) because the permit is held
        assertThat(writeCompleted.get())
            .as("Write should be blocked waiting for a permit")
            .isFalse();

        // Release the permit by completing the hanging future
        hangingFuture.complete(UploadPartResponse.builder().eTag("etag-1").build());

        // Now the writer thread should unblock and complete
        writerThread.join(2000);
        assertThat(writeCompleted.get())
            .as("Write should complete after permit is released")
            .isTrue();

        // Clean up
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    @SuppressWarnings("resource")
    @Test
    @DisplayName("write throws IOException when a previous async upload failed")
    void writeThrowsIOExceptionOnAsyncFailure() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // First uploadPart returns a future that completes exceptionally
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("S3 upload failed"));
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // First write fills the buffer and triggers the failed upload
        ByteBuffer firstWrite = ByteBuffer.allocate(PART_SIZE);
        channel.write(firstWrite);

        // Give the future time to complete exceptionally
        Thread.sleep(50);

        // Next write should detect the failure and throw IOException
        ByteBuffer secondWrite = ByteBuffer.allocate(1);
        assertThatThrownBy(() -> channel.write(secondWrite))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("previous part upload failed");
    }

    @Test
    @DisplayName("close throws IOException when an in-flight upload failed")
    void closeThrowsIOExceptionOnInFlightFailure() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart returns a future that completes exceptionally
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("Network error"));
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // Write enough to trigger an upload
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // close() should detect the failed in-flight upload and throw IOException
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class)
            .hasMessageContaining("in-flight part upload failed");
    }

    @Test
    @DisplayName("semaphore permits are released on successful upload")
    void permitsReleasedOnSuccess() throws Exception {
        int maxInFlight = 1;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately with success
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // With maxInFlight=1, if permits weren't released, the second write would block forever.
        // Since uploads complete immediately, permits are released and subsequent writes proceed.
        ByteBuffer firstWrite = ByteBuffer.allocate(PART_SIZE);
        channel.write(firstWrite);

        // This should not block because the permit was released after the first upload completed
        ByteBuffer secondWrite = ByteBuffer.allocate(PART_SIZE);
        channel.write(secondWrite);

        // Third write should also succeed
        ByteBuffer thirdWrite = ByteBuffer.allocate(PART_SIZE);
        channel.write(thirdWrite);

        channel.close();
    }

    @SuppressWarnings("resource")
    @Test
    @DisplayName("semaphore permits are released on failed upload")
    void permitsReleasedOnFailure() throws Exception {
        int maxInFlight = 1;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // First upload fails
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("Upload failed"));

        // Second upload succeeds (if we get there)
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // Write enough to trigger the failed upload
        ByteBuffer firstWrite = ByteBuffer.allocate(PART_SIZE);
        channel.write(firstWrite);

        // Give the future time to complete
        Thread.sleep(50);

        // The permit should still be released even though the upload failed.
        // The next write will detect the failure via checkForAsyncFailures() and throw,
        // but the important thing is it doesn't deadlock waiting for a permit.
        ByteBuffer secondWrite = ByteBuffer.allocate(1);
        assertThatThrownBy(() -> channel.write(secondWrite))
            .isInstanceOf(IOException.class);
    }

    @Test
    @DisplayName("multiple concurrent uploads allowed up to maxInFlight")
    void multipleConcurrentUploadsAllowed() throws Exception {
        int maxInFlight = 3;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Use futures that don't complete immediately to simulate in-flight uploads
        var future1 = new CompletableFuture<UploadPartResponse>();
        var future2 = new CompletableFuture<UploadPartResponse>();
        var future3 = new CompletableFuture<UploadPartResponse>();
        var future4 = new CompletableFuture<UploadPartResponse>();

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(future1)
            .thenReturn(future2)
            .thenReturn(future3)
            .thenReturn(future4);

        // Write 3 full buffers - should not block since maxInFlight=3
        for (int i = 0; i < 3; i++) {
            ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
            channel.write(data);
        }

        // 4th write should block because all 3 permits are taken
        var writeBlocked = new AtomicBoolean(true);
        var blockedLatch = new CountDownLatch(1);

        Thread writerThread = new Thread(() -> {
            try {
                blockedLatch.countDown();
                ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
                channel.write(data);
                writeBlocked.set(false);
            } catch (IOException e) {
                // Expected
            }
        });

        writerThread.start();
        blockedLatch.await(2, TimeUnit.SECONDS);
        Thread.sleep(200);

        // Should still be blocked
        assertThat(writeBlocked.get())
            .as("4th write should block when all 3 permits are taken")
            .isTrue();

        // Release one permit
        future1.complete(UploadPartResponse.builder().eTag("etag-1").build());

        writerThread.join(2000);
        assertThat(writeBlocked.get())
            .as("Write should unblock after one permit is released")
            .isFalse();

        // Complete remaining futures for cleanup
        future2.complete(UploadPartResponse.builder().eTag("etag-2").build());
        future3.complete(UploadPartResponse.builder().eTag("etag-3").build());
        future4.complete(UploadPartResponse.builder().eTag("etag-4").build());

        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("close propagates failure from in-flight uploads and aborts the session")
    void closePropagatesFailureAndAborts() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // First upload succeeds, second fails
        var successFuture = CompletableFuture.completedFuture(
            UploadPartResponse.builder().eTag("etag-1").build());
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("Part upload timeout"));

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(successFuture)
            .thenReturn(failedFuture);

        // Trigger two uploads
        ByteBuffer data1 = ByteBuffer.allocate(PART_SIZE);
        channel.write(data1);
        ByteBuffer data2 = ByteBuffer.allocate(PART_SIZE);
        channel.write(data2);

        // close() should detect the failure during drainInFlightUploads and throw
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class)
            .hasMessageContaining("in-flight part upload failed");

        // Channel should be closed after the exception
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    @DisplayName("Small write that doesn't fill buffer should not block")
    void writeDoesNotBlockWhenBufferNotFull() throws Exception {
        int maxInFlight = 1;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Use a hanging future to hold the permit
        var hangingFuture = new CompletableFuture<UploadPartResponse>();
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(hangingFuture);

        // First write fills the buffer and triggers upload (takes the permit)
        ByteBuffer firstWrite = ByteBuffer.allocate(PART_SIZE);
        channel.write(firstWrite);

        // A small write that doesn't fill the buffer should NOT block
        // because no upload is triggered (no permit needed)
        ByteBuffer smallWrite = ByteBuffer.allocate(100);
        var writeCompleted = new AtomicBoolean(false);
        var exceptionRef = new AtomicReference<Exception>();

        Thread writerThread = new Thread(() -> {
            try {
                channel.write(smallWrite);
                writeCompleted.set(true);
            } catch (IOException e) {
                exceptionRef.set(e);
            }
        });

        writerThread.start();
        writerThread.join(1000);

        assertThat(writeCompleted.get())
            .as("Small write that doesn't fill buffer should not block")
            .isTrue();

        // Clean up
        hangingFuture.complete(UploadPartResponse.builder().eTag("etag-1").build());
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    // ---- Fallback to Temp-File Mode Tests ----

    @Test
    @DisplayName("backward seek triggers abort and switches to RANDOM_ACCESS mode")
    void backwardSeekTriggersAbortAndSwitchesToRandomAccess() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write enough to trigger one part upload
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        for (int i = 0; i < PART_SIZE; i++) {
            data.put((byte) (i % 127));
        }
        data.flip();
        channel.write(data);

        // Verify we're in APPEND_ONLY mode
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.APPEND_ONLY);

        // Backward seek should trigger fallback
        channel.position(0);

        // Verify mode switched to RANDOM_ACCESS
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.RANDOM_ACCESS);

        // Verify abort was called
        verify(mockClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("after fallback, temp file contains correct data from uploaded parts")
    void fallbackTempFileContainsCorrectData() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write a full part with known data
        byte[] partData = new byte[PART_SIZE];
        for (int i = 0; i < PART_SIZE; i++) {
            partData[i] = (byte) (i % 127);
        }
        channel.write(ByteBuffer.wrap(partData));

        // Write some additional data to the current buffer (not yet uploaded)
        byte[] bufferData = new byte[100];
        for (int i = 0; i < 100; i++) {
            bufferData[i] = (byte) (i + 50);
        }
        channel.write(ByteBuffer.wrap(bufferData));

        // Trigger fallback
        channel.position(0);

        // Read the temp file and verify its content
        var tempFilePath = channel.getTempFile();
        assertThat(tempFilePath).isNotNull();

        byte[] tempFileContent = Files.readAllBytes(tempFilePath);
        assertThat(tempFileContent.length).isEqualTo(PART_SIZE + 100);

        // Verify the first part data
        for (int i = 0; i < PART_SIZE; i++) {
            assertThat(tempFileContent[i]).isEqualTo((byte) (i % 127));
        }

        // Verify the buffer data
        for (int i = 0; i < 100; i++) {
            assertThat(tempFileContent[PART_SIZE + i]).isEqualTo((byte) (i + 50));
        }

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("after fallback, writes go to temp file")
    void afterFallbackWritesGoToTempFile() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write a full part
        byte[] originalData = new byte[PART_SIZE];
        for (int i = 0; i < PART_SIZE; i++) {
            originalData[i] = (byte) 'A';
        }
        channel.write(ByteBuffer.wrap(originalData));

        // Trigger fallback to position 0
        channel.position(0);

        // Write new data at position 0 (overwriting)
        byte[] newData = new byte[10];
        for (int i = 0; i < 10; i++) {
            newData[i] = (byte) 'B';
        }
        channel.write(ByteBuffer.wrap(newData));

        // Read the temp file and verify the overwrite
        var tempFilePath = channel.getTempFile();
        byte[] tempFileContent = Files.readAllBytes(tempFilePath);

        // First 10 bytes should be 'B' (overwritten)
        for (int i = 0; i < 10; i++) {
            assertThat(tempFileContent[i]).isEqualTo((byte) 'B');
        }

        // Remaining bytes should still be 'A'
        for (int i = 10; i < PART_SIZE; i++) {
            assertThat(tempFileContent[i]).isEqualTo((byte) 'A');
        }

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("close in random-access mode uploads temp file via PutObject")
    void closeInRandomAccessModeUploadsTempFile() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // putObject should succeed
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));

        // Write a full part
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Trigger fallback
        channel.position(0);

        // Close should upload via PutObject
        channel.close();

        // Verify putObject was called
        verify(mockClient).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

        // Verify completeMultipartUpload was NOT called (we aborted)
        verify(mockClient, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    @DisplayName("forward seek triggers fallback to temp-file mode")
    void forwardSeekTriggersFallback() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write some data
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        // Forward seek should trigger fallback (creates a gap that can't be represented in streaming mode)
        channel.position(200);

        // Verify mode switched to RANDOM_ACCESS
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.RANDOM_ACCESS);

        // Verify abort was called (multipart session aborted)
        verify(mockClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("forward seek across part boundaries triggers fallback and preserves data")
    void forwardSeekAcrossPartBoundariesPreservesData() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write some initial data (less than one part)
        byte[] initialData = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            initialData[i] = (byte) (i % 127);
        }
        channel.write(ByteBuffer.wrap(initialData));

        // Forward seek past multiple part boundaries
        long seekTarget = (long) PART_SIZE * 3;
        channel.position(seekTarget);

        // Verify fallback occurred
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.RANDOM_ACCESS);

        // Verify temp file has the initial data at the beginning
        var tempFilePath = channel.getTempFile();
        byte[] tempFileContent = Files.readAllBytes(tempFilePath);
        assertThat(tempFileContent.length).isEqualTo(1000);
        for (int i = 0; i < 1000; i++) {
            assertThat(tempFileContent[i]).isEqualTo((byte) (i % 127));
        }

        // Verify position is at the seek target
        assertThat(channel.position()).isEqualTo(seekTarget);

        // Write after the gap — this goes to the temp file at the seeked position
        byte[] afterGapData = new byte[]{42, 43, 44};
        channel.write(ByteBuffer.wrap(afterGapData));

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("position(currentPosition) does not trigger fallback (no-op)")
    void positionAtCurrentPositionDoesNotTriggerFallback() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write some data
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        // Setting position to current position should be a no-op
        channel.position(100);

        // Verify still in APPEND_ONLY mode
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.APPEND_ONLY);

        // Verify abort was NOT called
        verify(mockClient, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        // Clean up
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("forward seek by 1 byte triggers fallback")
    void forwardSeekByOneByteTrigersFallback() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write some data
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        // Even a 1-byte forward seek triggers fallback
        channel.position(101);

        // Verify mode switched to RANDOM_ACCESS
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.RANDOM_ACCESS);

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("size delegates to temp file channel in random-access mode")
    void sizeDelegatesToTempFileInRandomAccessMode() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write a full part
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Trigger fallback
        channel.position(0);

        // Size should reflect the temp file size (which is PART_SIZE)
        assertThat(channel.size()).isEqualTo(PART_SIZE);

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("position in random-access mode delegates to temp file channel")
    void positionInRandomAccessModeDelegatesToTempFile() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write a full part
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Trigger fallback to position 0
        channel.position(0);
        assertThat(channel.position()).isEqualTo(0);

        // Seek forward within the temp file
        channel.position(100);
        assertThat(channel.position()).isEqualTo(100);

        // Seek backward within the temp file (allowed in RANDOM_ACCESS mode)
        channel.position(50);
        assertThat(channel.position()).isEqualTo(50);

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("close in random-access mode throws IOException on upload failure")
    void closeInRandomAccessModeThrowsOnUploadFailure() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // putObject fails
        var failedFuture = new CompletableFuture<PutObjectResponse>();
        failedFuture.completeExceptionally(new RuntimeException("PutObject failed"));
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // Write a full part
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Trigger fallback
        channel.position(0);

        // Close should throw IOException due to PutObject failure
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Failed to upload temp file");
    }

    @Test
    @DisplayName("fallback with no prior uploads creates temp file with only buffer data")
    void fallbackWithNoUploadsCreatesEmptyTempFile() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write some data that doesn't fill the buffer (no upload triggered)
        byte[] smallData = new byte[100];
        for (int i = 0; i < 100; i++) {
            smallData[i] = (byte) (i + 1);
        }
        channel.write(ByteBuffer.wrap(smallData));

        // Trigger fallback
        channel.position(0);

        // Verify mode switched
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.RANDOM_ACCESS);

        // Verify temp file contains the buffer data
        var tempFilePath = channel.getTempFile();
        byte[] tempFileContent = Files.readAllBytes(tempFilePath);
        assertThat(tempFileContent.length).isEqualTo(100);
        for (int i = 0; i < 100; i++) {
            assertThat(tempFileContent[i]).isEqualTo((byte) (i + 1));
        }

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("fallback drains in-flight uploads best-effort ignoring failures")
    void fallbackDrainsInFlightBestEffort() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // First upload fails
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("Upload failed"));
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // Write a full part (triggers the failed upload)
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Give the future time to complete
        Thread.sleep(50);

        // Trigger fallback - should not throw despite the failed in-flight upload
        channel.position(0);

        // Verify mode switched to RANDOM_ACCESS
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.RANDOM_ACCESS);

        // Clean up
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("read throws NonReadableChannelException")
    void readThrowsNonReadableChannelException() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(10)))
            .isInstanceOf(java.nio.channels.NonReadableChannelException.class);

        channel.close();
    }

    @Test
    @DisplayName("truncate throws UnsupportedOperationException")
    void truncateThrowsUnsupportedOperationException() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        assertThatThrownBy(() -> channel.truncate(100))
            .isInstanceOf(UnsupportedOperationException.class);

        channel.close();
    }

    @Test
    @DisplayName("write on closed channel throws ClosedChannelException")
    void writeOnClosedChannelThrowsClosedChannelException() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        channel.close();

        assertThatThrownBy(() -> channel.write(ByteBuffer.allocate(10)))
            .isInstanceOf(java.nio.channels.ClosedChannelException.class);
    }

    @Test
    @DisplayName("position with negative value throws IllegalArgumentException")
    void positionWithNegativeValueThrows() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        assertThatThrownBy(() -> channel.position(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("negative");

        channel.close();
    }

    @Test
    @DisplayName("close without any writes does not initiate multipart session")
    void closeWithoutWritesDoesNotInitiateSession() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        channel.close();

        // Verify no S3 calls were made
        verify(mockClient, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockClient, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    @DisplayName("close is idempotent - second close has no effect")
    void closeIsIdempotent() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // Write some data
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        // First close
        channel.close();
        assertThat(channel.isOpen()).isFalse();

        // Second close should not throw
        channel.close();
    }

    @Test
    @DisplayName("write with empty buffer returns 0 in append-only mode")
    void writeWithEmptyBufferReturnsZero() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write with empty buffer
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        int bytesWritten = channel.write(emptyBuffer);
        assertThat(bytesWritten).isEqualTo(0);

        channel.close();
    }

    @Test
    @DisplayName("position getter returns current position")
    void positionGetterReturnsCurrentPosition() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        assertThat(channel.position()).isEqualTo(0);

        // Set up uploadPart mock for the flush on close
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write some data (less than part size, so no upload triggered during write)
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        assertThat(channel.position()).isEqualTo(100);

        // Clean up
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    // ---- Error Handling and Cleanup Tests (Task 6) ----

    @Test
    @DisplayName("upload failure triggers abort with upload ID and part count logging")
    void uploadFailureTriggersAbort() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // First upload succeeds, second fails
        var successFuture = CompletableFuture.completedFuture(
            UploadPartResponse.builder().eTag("etag-1").build());
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("S3 error"));

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(successFuture)
            .thenReturn(failedFuture);

        // Trigger two uploads
        ByteBuffer data1 = ByteBuffer.allocate(PART_SIZE);
        channel.write(data1);
        ByteBuffer data2 = ByteBuffer.allocate(PART_SIZE);
        channel.write(data2);

        // close() should detect the failure and call abort
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class);

        // Verify abort was called
        verify(mockClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    @DisplayName("CompleteMultipartUpload failure triggers abort then throws IOException")
    void completeFailureTriggersAbortThenThrows() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart succeeds
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // completeMultipartUpload fails
        var failedComplete = new CompletableFuture<CompleteMultipartUploadResponse>();
        failedComplete.completeExceptionally(new RuntimeException("Complete failed"));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(failedComplete);

        // Write some data
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        // close() should throw IOException and call abort
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Failed to complete multipart upload");

        // Verify abort was called after complete failure
        verify(mockClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    @DisplayName("shutdown hook is registered on first write")
    void shutdownHookRegisteredOnFirstWrite() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Before any write, no shutdown hook
        assertThat(channel.getShutdownHook()).isNull();

        // Write triggers multipart upload initiation which registers the hook
        ByteBuffer data = ByteBuffer.allocate(10);
        channel.write(data);

        // Shutdown hook should now be registered
        assertThat(channel.getShutdownHook()).isNotNull();

        // Clean up
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("shutdown hook is deregistered on successful close")
    void shutdownHookDeregisteredOnSuccessfulClose() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart succeeds
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // Write data to trigger session initiation
        ByteBuffer data = ByteBuffer.allocate(10);
        channel.write(data);

        // Verify hook is registered
        assertThat(channel.getShutdownHook()).isNotNull();

        // Close successfully
        channel.close();

        // Verify hook is deregistered
        assertThat(channel.getShutdownHook()).isNull();
    }

    @Test
    @DisplayName("timeout retry: upload retries once on timeout then succeeds")
    void timeoutRetrySucceedsOnSecondAttempt() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // First call times out, second call succeeds
        var timeoutFuture = new CompletableFuture<UploadPartResponse>();
        timeoutFuture.completeExceptionally(new java.util.concurrent.TimeoutException("Request timed out"));

        var successFuture = CompletableFuture.completedFuture(
            UploadPartResponse.builder().eTag("etag-1").build());

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(timeoutFuture)
            .thenReturn(successFuture);

        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // Write a full part to trigger upload
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Close should succeed because the retry worked
        channel.close();

        // Verify uploadPart was called twice (original + retry)
        verify(mockClient, org.mockito.Mockito.times(2))
            .uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
    }

    @Test
    @DisplayName("timeout retry: upload fails after retry also times out")
    void timeoutRetryFailsAfterSecondTimeout() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Both calls time out
        var timeoutFuture1 = new CompletableFuture<UploadPartResponse>();
        timeoutFuture1.completeExceptionally(new java.util.concurrent.TimeoutException("Request timed out"));

        var timeoutFuture2 = new CompletableFuture<UploadPartResponse>();
        timeoutFuture2.completeExceptionally(new java.util.concurrent.TimeoutException("Retry also timed out"));

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(timeoutFuture1)
            .thenReturn(timeoutFuture2);

        // Write a full part to trigger upload
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Give futures time to complete
        Thread.sleep(50);

        // close() should detect the failure and throw
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class);

        // Verify abort was called
        verify(mockClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    @DisplayName("non-timeout failure does not trigger retry")
    void nonTimeoutFailureDoesNotRetry() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Upload fails with a non-timeout error
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("Access denied"));

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // Write a full part to trigger upload
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Give future time to complete
        Thread.sleep(50);

        // close() should detect the failure
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class);

        // Verify uploadPart was called only once (no retry for non-timeout)
        verify(mockClient, org.mockito.Mockito.times(1))
            .uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
    }

    // ---- Part Limit Enforcement Tests (Task 7) ----

    @Test
    @DisplayName("part limit exceeded throws IllegalStateException at 10,001st part")
    void partLimitExceeded_throwsIllegalStateException() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write first part to initiate the session
        channel.write(ByteBuffer.allocate(PART_SIZE));

        // Use reflection to set nextPartNumber to MAX_PARTS + 1 (simulating 10,000 parts already uploaded)
        var field = S3StreamingMultipartUploadChannel.class.getDeclaredField("nextPartNumber");
        field.setAccessible(true);
        field.setInt(channel, S3StreamingMultipartUpload.MAX_PARTS + 1);

        // Next write that fills the buffer should throw IllegalStateException
        assertThatThrownBy(() -> channel.write(ByteBuffer.allocate(PART_SIZE)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("10000")
            .hasMessageContaining(String.valueOf(PART_SIZE));
    }

    @Test
    @DisplayName("part limit exceeded triggers abort of multipart upload")
    void partLimitExceeded_triggersAbort() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write first part to initiate the session
        channel.write(ByteBuffer.allocate(PART_SIZE));

        // Use reflection to set nextPartNumber to MAX_PARTS + 1
        var field = S3StreamingMultipartUploadChannel.class.getDeclaredField("nextPartNumber");
        field.setAccessible(true);
        field.setInt(channel, S3StreamingMultipartUpload.MAX_PARTS + 1);

        // Trigger the part limit check
        try {
            channel.write(ByteBuffer.allocate(PART_SIZE));
        } catch (IllegalStateException e) {
            // Expected
        }

        // Verify abort was called
        verify(mockClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    @DisplayName("part limit error message contains part size and total bytes written")
    void partLimitExceeded_errorMessageContainsDetails() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart completes immediately
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        // Write first part to initiate the session
        channel.write(ByteBuffer.allocate(PART_SIZE));

        // Use reflection to set nextPartNumber to MAX_PARTS + 1
        var field = S3StreamingMultipartUploadChannel.class.getDeclaredField("nextPartNumber");
        field.setAccessible(true);
        field.setInt(channel, S3StreamingMultipartUpload.MAX_PARTS + 1);

        // Next write that fills the buffer should throw with descriptive message
        assertThatThrownBy(() -> channel.write(ByteBuffer.allocate(PART_SIZE)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining(String.valueOf(S3StreamingMultipartUpload.MAX_PARTS))
            .hasMessageContaining(String.valueOf(PART_SIZE))
            .hasMessageContaining("total bytes written")
            .hasMessageContaining("Consider increasing the part size");
    }

    @Test
    @DisplayName("abort failure does not throw, just logs warning")
    void abortFailureDoesNotThrow() throws Exception {
        int maxInFlight = 2;
        var option = new S3StreamingMultipartUpload(PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // uploadPart fails
        var failedFuture = new CompletableFuture<UploadPartResponse>();
        failedFuture.completeExceptionally(new RuntimeException("Upload error"));
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // abortMultipartUpload also fails
        var abortFailedFuture = new CompletableFuture<AbortMultipartUploadResponse>();
        abortFailedFuture.completeExceptionally(new RuntimeException("Abort also failed"));
        when(mockClient.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
            .thenReturn(abortFailedFuture);

        // Write a full part to trigger the failed upload
        ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
        channel.write(data);

        // Give future time to complete
        Thread.sleep(50);

        // close() should throw IOException for the upload failure,
        // but the abort failure should not cause an additional exception
        assertThatThrownBy(() -> channel.close())
            .isInstanceOf(IOException.class)
            .hasMessageContaining("in-flight part upload failed");

        // Verify abort was attempted
        verify(mockClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        // Channel should be closed (no additional exception from abort failure)
        assertThat(channel.isOpen()).isFalse();
    }

    // ---- Fallback Disabled (Strict Append-Only Mode) Tests ----

    @Test
    @DisplayName("fallback disabled: seek throws UnsupportedOperationException")
    void fallbackDisabled_seekThrowsUnsupportedOperationException() throws Exception {
        var option = new S3StreamingMultipartUpload(PART_SIZE, 2, false);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // Write some data
        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        // Any position change should throw
        assertThatThrownBy(() -> channel.position(0))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("strict append-only mode")
            .hasMessageContaining("fallback disabled");

        // Forward seek should also throw
        assertThatThrownBy(() -> channel.position(200))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("strict append-only mode");

        // Channel should still be in APPEND_ONLY mode (no fallback occurred)
        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.APPEND_ONLY);

        channel.close();
    }

    @Test
    @DisplayName("fallback disabled: position(currentPosition) is still a no-op")
    void fallbackDisabled_positionAtCurrentIsNoOp() throws Exception {
        var option = new S3StreamingMultipartUpload(PART_SIZE, 2, false);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        ByteBuffer data = ByteBuffer.allocate(100);
        channel.write(data);

        // Setting position to current value should not throw
        channel.position(100);

        assertThat(channel.getMode()).isEqualTo(S3StreamingMultipartUploadChannel.Mode.APPEND_ONLY);
        assertThat(channel.position()).isEqualTo(100);

        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    @Test
    @DisplayName("fallback disabled: part data is not retained in memory after upload")
    void fallbackDisabled_partDataNotRetained() throws Exception {
        var option = new S3StreamingMultipartUpload(PART_SIZE, 2, false);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // Write multiple full parts
        for (int i = 0; i < 3; i++) {
            ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
            channel.write(data);
        }

        // Use reflection to check partDataHistory is empty
        var field = S3StreamingMultipartUploadChannel.class.getDeclaredField("partDataHistory");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<byte[]> history = (List<byte[]>) field.get(channel);
        assertThat(history).isEmpty();

        channel.close();
    }

    @Test
    @DisplayName("fallback disabled: normal append-only write and close works correctly")
    void fallbackDisabled_normalWriteAndCloseWorks() throws Exception {
        var option = new S3StreamingMultipartUpload(PART_SIZE, 2, false);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // Write 2.5 parts worth of data
        for (int i = 0; i < 2; i++) {
            ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
            channel.write(data);
        }
        ByteBuffer remainder = ByteBuffer.allocate(PART_SIZE / 2);
        channel.write(remainder);

        channel.close();

        // Verify complete was called (upload succeeded)
        verify(mockClient).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    @DisplayName("fallback enabled (default): part data is retained for fallback")
    void fallbackEnabled_partDataRetained() throws Exception {
        var option = new S3StreamingMultipartUpload(PART_SIZE, 2, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        // Write multiple full parts
        for (int i = 0; i < 3; i++) {
            ByteBuffer data = ByteBuffer.allocate(PART_SIZE);
            channel.write(data);
        }

        // Use reflection to check partDataHistory has entries
        var field = S3StreamingMultipartUploadChannel.class.getDeclaredField("partDataHistory");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<byte[]> history = (List<byte[]>) field.get(channel);
        assertThat(history).hasSize(3);

        channel.close();
    }
}
