/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

/**
 * A write-only {@link SeekableByteChannel} that uploads data to S3 incrementally using the multipart upload API.
 *
 * <p>
 * Data is accumulated in a {@link PartBuffer} until the configured part size is reached, at which point the buffer
 * is uploaded asynchronously as a numbered part. On {@link #close()}, any remaining buffered data is flushed as the
 * final part and the multipart upload is completed.
 *
 * <p>
 * This channel operates in append-only mode by default. If a backward seek is detected, it falls back to
 * the existing temp-file approach.
 */
class S3StreamingMultipartUploadChannel implements SeekableByteChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamingMultipartUploadChannel.class);

    /**
     * The operating mode of the channel.
     */
    enum Mode {
        /** Sequential append-only writes; parts are uploaded as they fill. */
        APPEND_ONLY,
        /** Random-access mode; delegates to a temp-file channel after a backward seek. */
        RANDOM_ACCESS
    }

    // Configuration
    private final S3Path s3Path;
    private final S3AsyncClient s3Client;
    private final int partSize;
    private final int maxInFlight;
    private final boolean fallbackEnabled;

    // Channel state
    private Mode mode = Mode.APPEND_ONLY;
    private boolean open = true;
    private long position = 0;

    // Multipart upload state
    private String uploadId;
    private int nextPartNumber = 1;
    private final List<CompletedPart> completedParts = new ArrayList<>();
    private PartBuffer currentBuffer;

    // Backpressure
    private final Semaphore inFlightPermits;
    private final Queue<CompletableFuture<CompletedPart>> inFlightUploads = new ConcurrentLinkedQueue<>();

    // Shutdown hook
    private Thread shutdownHook;

    // Fallback state (only populated when fallbackEnabled is true)
    private final List<byte[]> partDataHistory = new ArrayList<>();
    private Path tempFile;
    private FileChannel tempFileChannel;

    /**
     * Creates a new streaming multipart upload channel.
     *
     * @param s3Path the S3 path (bucket + key) to upload to
     * @param s3Client the async S3 client for performing multipart operations
     * @param option the streaming multipart upload configuration (part size and max in-flight)
     */
    S3StreamingMultipartUploadChannel(S3Path s3Path, S3AsyncClient s3Client, S3StreamingMultipartUpload option) {
        this.s3Path = s3Path;
        this.s3Client = s3Client;
        this.partSize = (int) option.getPartSize();
        this.maxInFlight = option.getMaxInFlight();
        this.fallbackEnabled = option.isFallbackEnabled();
        this.inFlightPermits = new Semaphore(maxInFlight);
        this.currentBuffer = new PartBuffer(partSize);
    }

    /**
     * Writes a sequence of bytes from the given buffer to this channel.
     *
     * <p>
     * On the first write, a multipart upload session is initiated via {@code CreateMultipartUpload}. Bytes are
     * accumulated in the current {@link PartBuffer}. When the buffer is full, it is uploaded asynchronously as a
     * numbered part.
     *
     * @param src the source buffer containing bytes to write
     * @return the number of bytes written
     * @throws IOException if an I/O error occurs or a previous async upload failed
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        ensureOpen();

        // In random-access mode, delegate to temp file channel
        if (mode == Mode.RANDOM_ACCESS) {
            int bytesWritten = tempFileChannel.write(src);
            position = tempFileChannel.position();
            return bytesWritten;
        }

        checkForAsyncFailures();

        if (!src.hasRemaining()) {
            return 0;
        }

        // Initiate multipart upload on first write
        if (uploadId == null) {
            initiateMultipartUpload();
        }

        int totalBytesWritten = 0;

        while (src.hasRemaining()) {
            int bytesWritten = currentBuffer.write(src);
            totalBytesWritten += bytesWritten;
            position += bytesWritten;

            if (currentBuffer.isFull()) {
                uploadCurrentBuffer();
                currentBuffer = new PartBuffer(partSize);
            }
        }

        return totalBytesWritten;
    }

    /**
     * Closes this channel, flushing any remaining buffered data as the final part and completing the multipart upload.
     *
     * <p>
     * If no writes have occurred, no multipart session is initiated. This method is idempotent: calling it multiple
     * times has no additional effect after the first successful close.
     *
     * @throws IOException if an I/O error occurs during the final upload or completion
     */
    @Override
    public void close() throws IOException {
        if (!open) {
            return;
        }

        open = false;

        // In random-access mode, upload the temp file and clean up
        if (mode == Mode.RANDOM_ACCESS) {
            closeRandomAccessMode();
            return;
        }

        // If no writes occurred, nothing to do
        if (uploadId == null) {
            return;
        }

        try {
            // Wait for all in-flight uploads to complete
            drainInFlightUploads();

            // Flush remaining buffer as final part (even if smaller than partSize)
            flushRemainingBuffer();

            // Complete the multipart upload
            completeMultipartUpload();
        } catch (IOException e) {
            abortMultipartUpload();
            throw e;
        }
    }

    /**
     * Returns this channel's position, which is the total number of bytes written since the channel was opened.
     *
     * @return the current write position
     * @throws IOException if the channel is closed
     */
    @Override
    public long position() throws IOException {
        ensureOpen();
        return position;
    }

    /**
     * Sets this channel's position.
     *
     * <p>
     * In append-only mode, any explicit repositioning (forward or backward) that changes the current position
     * triggers a fallback to temp-file mode. A forward seek would create a "hole" (zero-filled gap) in the data
     * which cannot be represented correctly in a streaming multipart upload without materializing the zeros.
     * Rather than silently producing incorrect data or writing potentially large amounts of zero bytes through
     * the upload pipeline, the channel falls back to temp-file mode where sparse positioning is handled natively.
     *
     * <p>
     * In random-access mode, the position is delegated to the temp file channel.
     *
     * @param newPosition the new position
     * @return this channel
     * @throws IOException if the channel is closed or an I/O error occurs during fallback
     * @throws IllegalArgumentException if newPosition is negative
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        ensureOpen();

        if (newPosition < 0) {
            throw new IllegalArgumentException("newPosition cannot be negative");
        }

        if (mode == Mode.RANDOM_ACCESS) {
            tempFileChannel.position(newPosition);
            position = newPosition;
            return this;
        }

        if (newPosition != position) {
            if (!fallbackEnabled) {
                throw new UnsupportedOperationException(
                    "Position change not supported in strict append-only mode (fallback disabled). "
                        + "Current position: " + position + ", requested: " + newPosition);
            }
            // Any explicit repositioning (forward or backward) triggers fallback to temp-file mode.
            // Forward seeks create gaps that cannot be represented in streaming multipart upload
            // without zero-filling, which could be extremely expensive for large gaps.
            fallbackToTempFile(newPosition);
        }

        return this;
    }

    /**
     * Returns the current size of this channel. In append-only mode, this equals the current write position.
     * In random-access mode, this delegates to the temp file channel.
     *
     * @return the current size in bytes
     * @throws IOException if the channel is closed
     */
    @Override
    public long size() throws IOException {
        ensureOpen();
        if (mode == Mode.RANDOM_ACCESS) {
            return tempFileChannel.size();
        }
        return position;
    }

    /**
     * Always throws {@link NonReadableChannelException} because this is a write-only channel.
     *
     * @param dst the destination buffer (unused)
     * @return never returns normally
     * @throws NonReadableChannelException always
     */
    @Override
    public int read(ByteBuffer dst) throws NonReadableChannelException {
        throw new NonReadableChannelException();
    }

    /**
     * Always throws {@link UnsupportedOperationException} because truncation is not supported.
     *
     * @param size the new size (unused)
     * @return never returns normally
     * @throws UnsupportedOperationException always
     */
    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException("Truncate is not supported for streaming multipart upload channels");
    }

    /**
     * Completes the current multipart upload session and resets state so that subsequent writes
     * start a new multipart upload session. This allows data written so far to be persisted to S3
     * without closing the channel.
     *
     * <p>
     * In random-access mode, this forces the temp file channel and returns without completing
     * a multipart session.
     *
     * @throws IOException if an I/O error occurs during the completion
     * @throws ClosedChannelException if the channel is closed
     */
    void force() throws IOException {
        ensureOpen();

        if (mode == Mode.RANDOM_ACCESS) {
            tempFileChannel.force(true);
            return;
        }

        // Nothing to force if no session started
        if (uploadId == null) {
            return;
        }

        // Complete current session
        drainInFlightUploads();
        flushRemainingBuffer();
        completeMultipartUpload();

        // Reset for new session
        uploadId = null;
        nextPartNumber = 1;
        completedParts.clear();
        partDataHistory.clear();
        currentBuffer = new PartBuffer(partSize);
    }

    /**
     * Returns whether this channel is open.
     *
     * @return {@code true} if the channel is open
     */
    @Override
    public boolean isOpen() {
        return open;
    }

    // ---- Internal methods ----

    /**
     * Initiates the multipart upload session by calling CreateMultipartUpload.
     * Registers a JVM shutdown hook to abort the upload if the session is still active.
     */
    private void initiateMultipartUpload() throws IOException {
        var request = CreateMultipartUploadRequest.builder()
            .bucket(s3Path.bucketName())
            .key(s3Path.getKey())
            .build();

        try {
            var response = s3Client.createMultipartUpload(request).get();
            uploadId = response.uploadId();
            LOGGER.debug("Initiated multipart upload for '{}' with uploadId '{}'", s3Path.toUri(), uploadId);
            registerShutdownHook();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while initiating multipart upload for: " + s3Path.toUri(), e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to initiate multipart upload for: " + s3Path.toUri(), e.getCause());
        }
    }

    /**
     * Uploads the current buffer as a part. Acquires a permit from the semaphore to enforce backpressure.
     * Saves a copy of the buffer data to partDataHistory for potential fallback.
     * If the upload times out, retries once before allowing the failure to propagate.
     */
    private void uploadCurrentBuffer() throws IOException {
        checkForAsyncFailures();

        // Enforce S3 part limit before initiating the upload
        if (nextPartNumber > S3StreamingMultipartUpload.MAX_PARTS) {
            abortMultipartUpload();
            throw new IllegalStateException(String.format(
                "S3 multipart upload part limit (%d) exceeded. Configured part size: %d bytes, "
                    + "total bytes written: %d. Consider increasing the part size.",
                S3StreamingMultipartUpload.MAX_PARTS, partSize, position));
        }

        try {
            inFlightPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for upload permit", e);
        }

        int partNumber = nextPartNumber++;
        ByteBuffer data = currentBuffer.flip();

        // Save a copy of the part data for potential fallback and retry.
        // When fallback is disabled, we still need the copy for timeout retry,
        // but we don't retain it in partDataHistory.
        byte[] dataCopy = new byte[data.remaining()];
        data.get(dataCopy);
        if (fallbackEnabled) {
            partDataHistory.add(dataCopy);
        }

        var request = UploadPartRequest.builder()
            .bucket(s3Path.bucketName())
            .key(s3Path.getKey())
            .uploadId(uploadId)
            .partNumber(partNumber)
            .contentLength((long) dataCopy.length)
            .build();

        var future = s3Client.uploadPart(request, AsyncRequestBody.fromBytes(dataCopy))
            .handle((response, error) -> {
                if (error != null && isTimeoutException(error)) {
                    LOGGER.warn("Part {} upload timed out for uploadId '{}', retrying once",
                        partNumber, uploadId);
                    return null; // Signal retry needed
                }
                if (error != null) {
                    throw new java.util.concurrent.CompletionException(error);
                }
                return response;
            })
            .thenCompose(response -> {
                if (response == null) {
                    // Retry the upload
                    return s3Client.uploadPart(request, AsyncRequestBody.fromBytes(dataCopy));
                }
                return CompletableFuture.completedFuture(response);
            })
            .thenApply(response -> {
                var completedPart = CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(response.eTag())
                    .build();
                LOGGER.debug("Uploaded part {} for uploadId '{}', eTag: {}", partNumber, uploadId, response.eTag());
                return completedPart;
            })
            .whenComplete((result, error) -> inFlightPermits.release());

        inFlightUploads.add(future);
    }

    /**
     * Flushes the remaining bytes in the current buffer as the final part.
     */
    private void flushRemainingBuffer() throws IOException {
        ByteBuffer data = currentBuffer.flip();
        if (!data.hasRemaining()) {
            return;
        }

        int partNumber = nextPartNumber++;

        var request = UploadPartRequest.builder()
            .bucket(s3Path.bucketName())
            .key(s3Path.getKey())
            .uploadId(uploadId)
            .partNumber(partNumber)
            .contentLength((long) data.remaining())
            .build();

        try {
            var response = s3Client.uploadPart(request, AsyncRequestBody.fromByteBuffer(data)).get();
            var completedPart = CompletedPart.builder()
                .partNumber(partNumber)
                .eTag(response.eTag())
                .build();
            completedParts.add(completedPart);
            LOGGER.debug("Uploaded final part {} for uploadId '{}', eTag: {}", partNumber, uploadId, response.eTag());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while uploading final part", e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to upload final part for uploadId: " + uploadId, e.getCause());
        }
    }

    /**
     * Completes the multipart upload by calling CompleteMultipartUpload with all part ETags in order.
     * Deregisters the shutdown hook on success.
     */
    private void completeMultipartUpload() throws IOException {
        var completedUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        var request = CompleteMultipartUploadRequest.builder()
            .bucket(s3Path.bucketName())
            .key(s3Path.getKey())
            .uploadId(uploadId)
            .multipartUpload(completedUpload)
            .build();

        try {
            s3Client.completeMultipartUpload(request).get();
            LOGGER.debug("Completed multipart upload for '{}' with uploadId '{}'", s3Path.toUri(), uploadId);
            deregisterShutdownHook();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while completing multipart upload for uploadId: " + uploadId, e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to complete multipart upload for uploadId: " + uploadId, e.getCause());
        }
    }

    /**
     * Aborts the multipart upload session. Best-effort: logs a warning if abort itself fails.
     * Logs the upload ID and the number of successfully completed parts before the failure.
     */
    private void abortMultipartUpload() {
        if (uploadId == null) {
            return;
        }

        LOGGER.warn("Aborting multipart upload for uploadId '{}'. Successfully completed parts: {}",
            uploadId, completedParts.size());

        var request = AbortMultipartUploadRequest.builder()
            .bucket(s3Path.bucketName())
            .key(s3Path.getKey())
            .uploadId(uploadId)
            .build();

        try {
            s3Client.abortMultipartUpload(request).get();
            LOGGER.debug("Aborted multipart upload for '{}' with uploadId '{}'", s3Path.toUri(), uploadId);
            deregisterShutdownHook();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while aborting multipart upload for uploadId '{}'. "
                + "Manual cleanup may be required.", uploadId);
        } catch (ExecutionException e) {
            LOGGER.warn("Failed to abort multipart upload for uploadId '{}'. "
                + "Manual cleanup may be required. Error: {}", uploadId, e.getCause().getMessage());
        }
    }

    /**
     * Waits for all in-flight uploads to complete and collects their CompletedPart results.
     */
    private void drainInFlightUploads() throws IOException {
        CompletableFuture<CompletedPart> future;
        while ((future = inFlightUploads.poll()) != null) {
            try {
                completedParts.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for in-flight uploads to complete", e);
            } catch (ExecutionException e) {
                throw new IOException("An in-flight part upload failed for uploadId: " + uploadId, e.getCause());
            }
        }

        // Sort completed parts by part number to ensure correct ordering
        completedParts.sort((a, b) -> Integer.compare(a.partNumber(), b.partNumber()));
    }

    /**
     * Checks if any in-flight uploads have failed and propagates the failure as an IOException.
     */
    private void checkForAsyncFailures() throws IOException {
        for (var future : inFlightUploads) {
            if (future.isCompletedExceptionally()) {
                // Drain to get the actual exception
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while checking for async failures", e);
                } catch (ExecutionException e) {
                    throw new IOException("A previous part upload failed for uploadId: " + uploadId, e.getCause());
                }
            }
        }
    }

    /**
     * Falls back to temp-file mode when a backward seek is detected.
     *
     * <p>
     * This method:
     * <ol>
     *   <li>Logs a warning with the upload ID</li>
     *   <li>Drains in-flight uploads (ignoring failures since we're aborting)</li>
     *   <li>Aborts the active multipart upload session</li>
     *   <li>Creates a temp file and opens a FileChannel on it</li>
     *   <li>Writes all previously uploaded part data and current buffer content to the temp file</li>
     *   <li>Switches mode to RANDOM_ACCESS</li>
     *   <li>Positions the temp file channel at newPosition</li>
     * </ol>
     *
     * @param newPosition the position to seek to after fallback
     * @throws IOException if an I/O error occurs during fallback
     */
    private void fallbackToTempFile(long newPosition) throws IOException {
        LOGGER.warn("Non-sequential position change detected for uploadId '{}'. Falling back to temp-file mode. "
            + "Current position: {}, requested position: {}", uploadId, position, newPosition);

        // Drain in-flight uploads (best-effort, ignore failures since we're aborting)
        drainInFlightUploadsBestEffort();

        // Abort the active multipart upload session
        abortMultipartUpload();

        // Create temp file and open a FileChannel
        tempFile = Files.createTempFile("s3-streaming-", ".tmp");
        tempFile.toFile().deleteOnExit();
        tempFileChannel = FileChannel.open(tempFile,
            StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Write all previously uploaded part data to the temp file
        for (byte[] partData : partDataHistory) {
            tempFileChannel.write(ByteBuffer.wrap(partData));
        }

        // Write current buffer content to the temp file
        ByteBuffer currentData = currentBuffer.flip();
        if (currentData.hasRemaining()) {
            tempFileChannel.write(currentData);
        }

        // Switch mode
        mode = Mode.RANDOM_ACCESS;

        // Clear partDataHistory (no longer needed)
        partDataHistory.clear();

        // Position the temp file channel at newPosition
        tempFileChannel.position(newPosition);
        position = newPosition;
    }

    /**
     * Drains in-flight uploads best-effort, ignoring any failures.
     * Used during fallback when we're about to abort the multipart upload anyway.
     */
    private void drainInFlightUploadsBestEffort() {
        CompletableFuture<CompletedPart> future;
        while ((future = inFlightUploads.poll()) != null) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                // Ignore failures - we're aborting anyway
                LOGGER.debug("Ignoring in-flight upload failure during fallback: {}", e.getCause().getMessage());
            }
        }
    }

    /**
     * Closes the channel in random-access mode: uploads the temp file to S3 via PutObject,
     * then cleans up the temp file. Deregisters the shutdown hook since the multipart session
     * was already aborted during fallback.
     */
    private void closeRandomAccessMode() throws IOException {
        try {
            tempFileChannel.force(true);
            long fileSize = tempFileChannel.size();
            tempFileChannel.close();

            // Upload the temp file to S3 using PutObject
            var putRequest = PutObjectRequest.builder()
                .bucket(s3Path.bucketName())
                .key(s3Path.getKey())
                .contentLength(fileSize)
                .build();

            s3Client.putObject(putRequest, AsyncRequestBody.fromFile(tempFile)).get();
            LOGGER.debug("Uploaded temp file for '{}' via PutObject ({} bytes)", s3Path.toUri(), fileSize);
            deregisterShutdownHook();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while uploading temp file for: " + s3Path.toUri(), e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to upload temp file for: " + s3Path.toUri(), e.getCause());
        } finally {
            deleteTempFile();
        }
    }

    /**
     * Deletes the temp file if it exists. Best-effort: logs a warning on failure.
     */
    private void deleteTempFile() {
        if (tempFile != null) {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                LOGGER.warn("Failed to delete temp file '{}': {}", tempFile, e.getMessage());
            }
        }
    }

    /**
     * Ensures the channel is open, throwing ClosedChannelException if not.
     */
    private void ensureOpen() throws ClosedChannelException {
        if (!open) {
            throw new ClosedChannelException();
        }
    }

    /**
     * Registers a JVM shutdown hook that will attempt to abort the multipart upload
     * if the session is still active when the JVM shuts down.
     */
    private void registerShutdownHook() {
        shutdownHook = new Thread(() -> {
            if (open && uploadId != null) {
                LOGGER.warn("JVM shutting down with active multipart upload session. "
                    + "Attempting to abort uploadId '{}'", uploadId);
                abortMultipartUpload();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /**
     * Deregisters the JVM shutdown hook. Called after successful completion or abort.
     * Handles the case where the JVM is already shutting down gracefully.
     */
    private void deregisterShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // JVM is already shutting down, ignore
            }
            shutdownHook = null;
        }
    }

    /**
     * Determines if the given throwable is a timeout exception that warrants a retry.
     *
     * @param error the throwable to check
     * @return true if the error is a timeout exception
     */
    private boolean isTimeoutException(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof TimeoutException
                || cause instanceof ApiCallTimeoutException
                || cause instanceof ApiCallAttemptTimeoutException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    // ---- Package-private accessors for testing ----

    /**
     * Returns the current mode of the channel.
     */
    Mode getMode() {
        return mode;
    }

    /**
     * Returns the upload ID of the current multipart session, or null if not yet initiated.
     */
    String getUploadId() {
        return uploadId;
    }

    /**
     * Returns the next part number that will be assigned.
     */
    int getNextPartNumber() {
        return nextPartNumber;
    }

    /**
     * Returns the list of completed parts.
     */
    List<CompletedPart> getCompletedParts() {
        return completedParts;
    }

    /**
     * Returns the temp file path, or null if not in random-access mode.
     */
    Path getTempFile() {
        return tempFile;
    }

    /**
     * Returns the temp file channel, or null if not in random-access mode.
     */
    FileChannel getTempFileChannel() {
        return tempFileChannel;
    }

    /**
     * Returns the shutdown hook thread, or null if not registered.
     */
    Thread getShutdownHook() {
        return shutdownHook;
    }
}
