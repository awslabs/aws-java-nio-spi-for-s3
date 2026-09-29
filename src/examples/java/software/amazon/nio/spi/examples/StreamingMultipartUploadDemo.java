/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import software.amazon.nio.spi.s3.S3OpenOption;

/**
 * Demonstrates streaming multipart upload to S3 using the NIO SPI.
 *
 * <p>
 * This example shows how to upload large objects to S3 incrementally,
 * with parts being uploaded as data is written rather than buffering
 * everything to a local temp file.
 *
 * <p>
 * Prerequisites:
 * <ul>
 *   <li>AWS CRT client must be on the classpath</li>
 *   <li>AWS credentials configured (environment, profile, or IAM role)</li>
 * </ul>
 *
 * <p>
 * Usage: {@code java StreamingMultipartUploadDemo s3://my-bucket/path/to/object.dat}
 */
public class StreamingMultipartUploadDemo {

    // Simulated data chunk size for writing (64 KiB per iteration)
    private static final int WRITE_CHUNK_SIZE = 64 * 1024;

    // Total number of chunks to write (simulates a ~40 MiB upload = 5 parts at default 8 MiB)
    private static final int TOTAL_CHUNKS = 640;

    // Custom part size: 16 MiB (must be between 5 MiB and 5 GiB)
    private static final long CUSTOM_PART_SIZE = 16 * 1024 * 1024;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: StreamingMultipartUploadDemo <s3-uri>");
            System.err.println("  e.g. StreamingMultipartUploadDemo s3://my-bucket/uploads/large-file.dat");
            System.exit(1);
        }

        var s3Uri = args[0];

        // Example 1: Streaming upload with default settings (8 MiB parts, 4 concurrent uploads)
        uploadWithDefaults(s3Uri + "-default.dat");

        // Example 2: Streaming upload with a custom part size (16 MiB parts)
        uploadWithCustomPartSize(s3Uri + "-custom.dat");

        // Verify: read back both objects and check data integrity
        verifyUpload(s3Uri + "-default.dat");
        verifyUpload(s3Uri + "-custom.dat");
    }

    /**
     * Demonstrates a streaming multipart upload using default settings.
     *
     * <p>
     * Default configuration:
     * <ul>
     *   <li>Part size: 8 MiB — each part is uploaded when the buffer fills to this size</li>
     *   <li>Max in-flight: 4 — up to 4 parts upload concurrently for throughput</li>
     * </ul>
     */
    private static void uploadWithDefaults(String s3Uri) throws IOException {
        System.out.println("=== Streaming Upload with Default Settings ===");
        System.out.println("Target: " + s3Uri);

        var path = Paths.get(URI.create(s3Uri));

        // Step 1: Open a SeekableByteChannel with the streaming multipart upload option.
        // The streamingMultipartUpload() option tells the provider to upload parts
        // incrementally as data is written, instead of buffering to a temp file.
        try (SeekableByteChannel channel = Files.newByteChannel(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                S3OpenOption.streamingMultipartUpload())) {

            // Step 2: Write data in a loop. As the internal buffer fills to the
            // configured part size (8 MiB by default), parts are automatically
            // uploaded to S3 in the background.
            var buffer = ByteBuffer.allocate(WRITE_CHUNK_SIZE);
            for (int i = 0; i < TOTAL_CHUNKS; i++) {
                // Fill the buffer with sample data
                buffer.clear();
                fillBuffer(buffer, (byte) (i % 256));
                buffer.flip();

                // Write to the channel — when enough data accumulates, a part
                // upload is triggered automatically
                channel.write(buffer);
            }

            // Step 3: When the channel is closed (end of try-with-resources),
            // any remaining buffered data is flushed as the final part and
            // CompleteMultipartUpload is called to finalize the object in S3.
            System.out.println("Bytes written: " + channel.position());
        }

        System.out.println("Upload complete (default settings).\n");
    }

    /**
     * Demonstrates a streaming multipart upload with a custom part size.
     *
     * <p>
     * A larger part size means fewer S3 API calls but more memory usage per buffer.
     * Choose a part size based on your memory constraints and upload size:
     * <ul>
     *   <li>Minimum: 5 MiB (S3 requirement)</li>
     *   <li>Maximum: 5 GiB (S3 requirement)</li>
     *   <li>Larger parts = fewer API calls, higher memory usage</li>
     *   <li>Smaller parts = more API calls, lower memory usage</li>
     * </ul>
     */
    private static void uploadWithCustomPartSize(String s3Uri) throws IOException {
        System.out.println("=== Streaming Upload with Custom Part Size (16 MiB) ===");
        System.out.println("Target: " + s3Uri);

        var path = Paths.get(URI.create(s3Uri));

        // Step 1: Create the streaming option with a custom part size.
        // The factory method validates that the part size is within S3's
        // allowed range [5 MiB, 5 GiB]. An IllegalArgumentException is
        // thrown if the value is out of range.
        var streamingOption = S3OpenOption.streamingMultipartUpload(CUSTOM_PART_SIZE);

        // Step 2: Open the channel with the custom streaming option.
        try (SeekableByteChannel channel = Files.newByteChannel(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                streamingOption)) {

            // Step 3: Write data incrementally. With a 16 MiB part size,
            // parts are uploaded less frequently but each part is larger.
            // This can be more efficient for very large uploads where
            // reducing the number of S3 API calls is beneficial.
            var buffer = ByteBuffer.allocate(WRITE_CHUNK_SIZE);
            for (int i = 0; i < TOTAL_CHUNKS; i++) {
                buffer.clear();
                fillBuffer(buffer, (byte) (i % 256));
                buffer.flip();

                channel.write(buffer);

                // Optional: track progress by checking the channel position
                if (i > 0 && i % 128 == 0) {
                    System.out.printf("  Progress: %d MiB written%n", channel.position() / (1024 * 1024));
                }
            }

            // Step 4: Close flushes remaining data and completes the upload.
            System.out.println("Bytes written: " + channel.position());
        }

        System.out.println("Upload complete (custom part size).\n");
    }

    /**
     * Reads back an uploaded object and verifies every byte matches the expected pattern.
     * The demo writes chunk i with all bytes set to (byte)(i % 256), so we can reconstruct
     * the expected content and compare.
     */
    private static void verifyUpload(String s3Uri) throws IOException {
        System.out.println("=== Verifying: " + s3Uri + " ===");

        var path = Paths.get(URI.create(s3Uri));
        byte[] content = Files.readAllBytes(path);

        long expectedSize = (long) TOTAL_CHUNKS * WRITE_CHUNK_SIZE;
        if (content.length != expectedSize) {
            System.err.printf("  FAILED: expected %d bytes, got %d bytes%n", expectedSize, content.length);
            return;
        }

        // Verify each chunk has the expected byte pattern
        int errors = 0;
        for (int chunk = 0; chunk < TOTAL_CHUNKS; chunk++) {
            byte expectedValue = (byte) (chunk % 256);
            int offset = chunk * WRITE_CHUNK_SIZE;
            for (int j = 0; j < WRITE_CHUNK_SIZE; j++) {
                if (content[offset + j] != expectedValue) {
                    if (errors < 5) {
                        System.err.printf("  MISMATCH at byte %d: expected %d, got %d (chunk %d)%n",
                            offset + j, expectedValue & 0xFF, content[offset + j] & 0xFF, chunk);
                    }
                    errors++;
                }
            }
        }

        if (errors == 0) {
            System.out.printf("  PASSED: all %d bytes verified correctly (%d chunks of %d bytes)%n",
                content.length, TOTAL_CHUNKS, WRITE_CHUNK_SIZE);
        } else {
            System.err.printf("  FAILED: %d byte mismatches found%n", errors);
        }
    }

    /**
     * Fills a ByteBuffer with a repeating byte value.
     */
    private static void fillBuffer(ByteBuffer buffer, byte value) {
        while (buffer.hasRemaining()) {
            buffer.put(value);
        }
    }
}
