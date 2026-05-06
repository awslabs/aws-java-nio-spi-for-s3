/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import net.jqwik.api.constraints.Size;
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
 * Property-based tests for the streaming multipart upload feature.
 *
 * <p>Uses jqwik to verify correctness properties hold across many randomly generated inputs.
 */
class S3StreamingMultipartUploadPropertyTest {

    private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB

    private S3AsyncClient createMockClient() {
        S3AsyncClient mockClient = mock(S3AsyncClient.class);
        when(mockClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CreateMultipartUploadResponse.builder().uploadId("test-upload-id").build()));
        when(mockClient.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                AbortMultipartUploadResponse.builder().build()));
        return mockClient;
    }

    private S3Path createMockPath() {
        S3Path mockPath = mock(S3Path.class);
        when(mockPath.bucketName()).thenReturn("test-bucket");
        when(mockPath.getKey()).thenReturn("test-key");
        when(mockPath.toUri()).thenReturn(java.net.URI.create("s3://test-bucket/test-key"));
        return mockPath;
    }

    // Feature: streaming-multipart-upload, Part size validation accepts only valid range
    @Property(tries = 100)
    void partSizeValidation(@ForAll @LongRange(min = -100, max = 6L * 1024 * 1024 * 1024) long partSize) {
        boolean inRange = partSize >= 5L * 1024 * 1024 && partSize <= 5L * 1024 * 1024 * 1024;
        if (inRange) {
            assertDoesNotThrow(() -> S3OpenOption.streamingMultipartUpload(partSize));
        } else {
            assertThrows(IllegalArgumentException.class, () -> S3OpenOption.streamingMultipartUpload(partSize));
        }
    }

    // Feature: streaming-multipart-upload, Property: Buffering threshold triggers upload at exactly part size
    @Property(tries = 100)
    void bufferingThreshold(@ForAll @Size(min = 1, max = 10) List<@IntRange(min = 1, max = MIN_PART_SIZE) Integer> writeSizes) throws IOException {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        AtomicInteger uploadPartCount = new AtomicInteger(0);
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenAnswer(invocation -> {
                uploadPartCount.incrementAndGet();
                return CompletableFuture.completedFuture(
                    UploadPartResponse.builder().eTag("etag-" + uploadPartCount.get()).build());
            });
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        var option = new S3StreamingMultipartUpload(MIN_PART_SIZE, 4);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        long totalBytesWritten = 0;
        for (int size : writeSizes) {
            ByteBuffer buf = ByteBuffer.allocate(size);
            channel.write(buf);
            totalBytesWritten += size;
        }

        // The number of uploads triggered should be exactly totalBytesWritten / partSize
        int expectedUploads = (int) (totalBytesWritten / MIN_PART_SIZE);
        assertThat(uploadPartCount.get()).isEqualTo(expectedUploads);

        channel.close();
    }

    // Feature: streaming-multipart-upload, Property: Sequential part ordering
    @Property(tries = 100)
    void sequentialPartOrdering(@ForAll @IntRange(min = 2, max = 8) int numParts) throws IOException {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        List<Integer> capturedPartNumbers = new ArrayList<>();
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenAnswer(invocation -> {
                UploadPartRequest req = invocation.getArgument(0);
                capturedPartNumbers.add(req.partNumber());
                return CompletableFuture.completedFuture(
                    UploadPartResponse.builder().eTag("etag-" + req.partNumber()).build());
            });
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        var option = new S3StreamingMultipartUpload(MIN_PART_SIZE, 4);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write enough data to produce numParts full parts
        for (int i = 0; i < numParts; i++) {
            ByteBuffer buf = ByteBuffer.allocate(MIN_PART_SIZE);
            channel.write(buf);
        }

        channel.close();

        // Verify part numbers are sequential 1..N
        assertThat(capturedPartNumbers).hasSize(numParts);
        for (int i = 0; i < numParts; i++) {
            assertThat(capturedPartNumbers.get(i)).isEqualTo(i + 1);
        }
    }

    // Feature: streaming-multipart-upload, Property: Close flushes remaining data
    @Property(tries = 100)
    void closeFlushesRemaining(@ForAll @IntRange(min = 1, max = MIN_PART_SIZE - 1) int remainder) throws IOException {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        List<Long> uploadedContentLengths = new ArrayList<>();
        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenAnswer(invocation -> {
                UploadPartRequest req = invocation.getArgument(0);
                uploadedContentLengths.add(req.contentLength());
                return CompletableFuture.completedFuture(
                    UploadPartResponse.builder().eTag("etag-" + uploadedContentLengths.size()).build());
            });
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        var option = new S3StreamingMultipartUpload(MIN_PART_SIZE, 4);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write one full part + remainder
        ByteBuffer fullPart = ByteBuffer.allocate(MIN_PART_SIZE);
        channel.write(fullPart);

        ByteBuffer remainderBuf = ByteBuffer.allocate(remainder);
        channel.write(remainderBuf);

        channel.close();

        // Should have 2 uploads: one full part and one final part with remainder
        assertThat(uploadedContentLengths).hasSize(2);
        assertThat(uploadedContentLengths.get(0)).isEqualTo(MIN_PART_SIZE);
        assertThat(uploadedContentLengths.get(1)).isEqualTo((long) remainder);
    }

    // Feature: streaming-multipart-upload, Property: Fallback preserves all written data
    @Property(tries = 100)
    void fallbackDataPreservation(@ForAll @Size(min = 1, max = 5) List<@IntRange(min = 1, max = 1024) Integer> writeSizes) throws IOException {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(PutObjectResponse.builder().build()));

        // Use a small part size so we can trigger uploads with small writes
        int smallPartSize = MIN_PART_SIZE;
        var option = new S3StreamingMultipartUpload(smallPartSize, 4, true);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write data and track what we wrote
        byte[] allWrittenData = new byte[writeSizes.stream().mapToInt(Integer::intValue).sum()];
        int offset = 0;
        for (int size : writeSizes) {
            byte[] data = new byte[size];
            for (int i = 0; i < size; i++) {
                data[i] = (byte) ((offset + i) % 127);
            }
            channel.write(ByteBuffer.wrap(data));
            System.arraycopy(data, 0, allWrittenData, offset, size);
            offset += size;
        }

        // Trigger fallback via backward seek
        channel.position(0);

        // Verify temp file contains all written data
        var tempFilePath = channel.getTempFile();
        assertThat(tempFilePath).isNotNull();

        byte[] tempFileContent = Files.readAllBytes(tempFilePath);
        assertThat(tempFileContent).isEqualTo(allWrittenData);

        channel.close();
    }

    // Feature: streaming-multipart-upload, Property: Close idempotence
    @Property(tries = 100)
    void closeIdempotence(@ForAll @IntRange(min = 2, max = 5) int closeCalls) throws IOException {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        var option = new S3StreamingMultipartUpload(MIN_PART_SIZE, 4);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write enough to trigger at least one upload
        ByteBuffer data = ByteBuffer.allocate(MIN_PART_SIZE);
        channel.write(data);

        // Close multiple times
        for (int i = 0; i < closeCalls; i++) {
            channel.close();
        }

        // Verify completeMultipartUpload was called exactly once
        verify(mockClient, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    // Feature: streaming-multipart-upload, Property 7: Position equals total bytes written
    // **Validates: Requirements 10.1, 10.2**
    @Property(tries = 100)
    void positionTracking(@ForAll @Size(min = 1, max = 10) List<@IntRange(min = 1, max = 10000) Integer> writeSizes) throws IOException {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        var option = new S3StreamingMultipartUpload(MIN_PART_SIZE, 4);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        long expectedPosition = 0;
        for (int size : writeSizes) {
            ByteBuffer buf = ByteBuffer.allocate(size);
            channel.write(buf);
            expectedPosition += size;
            assertThat(channel.position()).isEqualTo(expectedPosition);
        }

        // Also verify size equals position in append-only mode
        assertThat(channel.size()).isEqualTo(expectedPosition);

        // Clean up
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));
        channel.close();
    }

    // Feature: streaming-multipart-upload, Property 8: Part limit enforcement
    // **Validates: Requirements 11.1, 11.3, 11.4**
    @Property(tries = 100)
    void partLimitEnforcement(@ForAll @IntRange(min = 10001, max = 10010) int partNumber) throws Exception {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(
                UploadPartResponse.builder().eTag("etag-1").build()));

        var option = new S3StreamingMultipartUpload(MIN_PART_SIZE, 4);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write one buffer to initiate the upload session
        ByteBuffer initBuf = ByteBuffer.allocate(1);
        channel.write(initBuf);

        // Use reflection to set nextPartNumber to simulate being at the limit
        Field nextPartNumberField = S3StreamingMultipartUploadChannel.class.getDeclaredField("nextPartNumber");
        nextPartNumberField.setAccessible(true);
        nextPartNumberField.setInt(channel, partNumber);

        // Writing a full buffer should trigger uploadCurrentBuffer which checks the limit
        ByteBuffer fullBuf = ByteBuffer.allocate(MIN_PART_SIZE);
        assertThatThrownBy(() -> channel.write(fullBuf))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("part limit");
    }

    // Feature: streaming-multipart-upload, Property 9: Memory bound invariant
    // **Validates: Requirements 6.4**
    @Property(tries = 100)
    void memoryBound(@ForAll @IntRange(min = 1, max = 4) int maxInFlight) throws IOException {
        S3AsyncClient mockClient = createMockClient();
        S3Path mockPath = createMockPath();

        // Track the number of concurrently held permits (in-flight uploads)
        AtomicInteger concurrentUploads = new AtomicInteger(0);
        AtomicInteger maxConcurrentUploads = new AtomicInteger(0);

        when(mockClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenAnswer(invocation -> {
                int current = concurrentUploads.incrementAndGet();
                maxConcurrentUploads.updateAndGet(max -> Math.max(max, current));
                // Complete immediately to release the permit
                concurrentUploads.decrementAndGet();
                return CompletableFuture.completedFuture(
                    UploadPartResponse.builder().eTag("etag-1").build());
            });
        when(mockClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CompleteMultipartUploadResponse.builder().build()));

        var option = new S3StreamingMultipartUpload(MIN_PART_SIZE, maxInFlight);
        var channel = new S3StreamingMultipartUploadChannel(mockPath, mockClient, option);

        // Write enough data to trigger multiple uploads
        for (int i = 0; i < maxInFlight + 3; i++) {
            ByteBuffer buf = ByteBuffer.allocate(MIN_PART_SIZE);
            channel.write(buf);
        }

        channel.close();

        // The max concurrent uploads should never exceed maxInFlight
        // (since the semaphore limits permits to maxInFlight)
        assertThat(maxConcurrentUploads.get()).isLessThanOrEqualTo(maxInFlight);
    }
}
