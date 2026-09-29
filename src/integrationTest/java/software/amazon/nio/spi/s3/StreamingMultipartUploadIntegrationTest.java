/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@DisplayName("StreamingMultipartUploadChannel integration tests with LocalStack")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StreamingMultipartUploadIntegrationTest {

    private static final int PART_SIZE = 5 * 1024 * 1024; // 5 MiB (minimum)

    private S3AsyncClient s3Client;
    private String bucketName;

    @BeforeEach
    void setUp() {
        bucketName = "streaming-upload-bucket" + System.currentTimeMillis();
        Containers.createBucket(bucketName);

        s3Client = S3AsyncClient.builder()
            .endpointOverride(Containers.LOCAL_STACK_CONTAINER.getEndpoint())
            .region(Region.of(Containers.LOCAL_STACK_CONTAINER.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    Containers.LOCAL_STACK_CONTAINER.getAccessKey(),
                    Containers.LOCAL_STACK_CONTAINER.getSecretKey())))
            .forcePathStyle(true)
            .build();
    }

    @Test
    @DisplayName("streaming upload of multi-part object, read back and verify content matches")
    void streamingUploadMultiPartObject_readBackVerifiesContent() throws Exception {
        var key = "multipart-test-object.bin";
        var s3Path = createS3Path(key);
        var option = new S3StreamingMultipartUpload(PART_SIZE, 4);

        // Write 2.5 parts worth of data (12.5 MiB) to trigger multiple part uploads
        int totalSize = (int) (PART_SIZE * 2.5);
        byte[] expectedData = createDeterministicData(totalSize);

        try (var channel = new S3StreamingMultipartUploadChannel(s3Path, s3Client, option)) {
            // Write in chunks to simulate realistic usage
            int chunkSize = 64 * 1024; // 64 KiB chunks
            ByteBuffer writeBuffer = ByteBuffer.allocate(chunkSize);
            int offset = 0;
            while (offset < totalSize) {
                int remaining = Math.min(chunkSize, totalSize - offset);
                writeBuffer.clear();
                writeBuffer.put(expectedData, offset, remaining);
                writeBuffer.flip();
                channel.write(writeBuffer);
                offset += remaining;
            }
        }

        // Read back the object and verify content matches
        byte[] actualData = readObject(key);
        assertThat(actualData).isEqualTo(expectedData);
    }

    @Test
    @DisplayName("fallback on backward seek produces correct object content")
    void fallbackOnBackwardSeek_producesCorrectContent() throws Exception {
        var key = "fallback-test-object.bin";
        var s3Path = createS3Path(key);
        var option = new S3StreamingMultipartUpload(PART_SIZE, 4, true);

        // Write enough data to fill at least one part, then seek backward
        int initialSize = PART_SIZE + (PART_SIZE / 2); // 1.5 parts
        byte[] initialData = createDeterministicData(initialSize);

        // Data to write after seeking back to position 0
        byte[] overwriteData = "OVERWRITTEN_DATA_AT_START".getBytes();

        try (var channel = new S3StreamingMultipartUploadChannel(s3Path, s3Client, option)) {
            // Write initial data
            channel.write(ByteBuffer.wrap(initialData));

            // Seek backward - triggers fallback to temp-file mode
            channel.position(0);

            // Write overwrite data at position 0
            channel.write(ByteBuffer.wrap(overwriteData));
        }

        // Build expected content: overwrite data at start, rest of initial data unchanged
        byte[] expectedData = Arrays.copyOf(initialData, initialSize);
        System.arraycopy(overwriteData, 0, expectedData, 0, overwriteData.length);

        // Read back and verify
        byte[] actualData = readObject(key);
        assertThat(actualData).isEqualTo(expectedData);
    }

    @Test
    @DisplayName("channel with default options uploads successfully")
    void channelWithDefaultOptions_uploadsSuccessfully() throws Exception {
        var key = "default-options-test.bin";
        var s3Path = createS3Path(key);
        // Use default streaming option (8 MiB part size, 4 max in-flight)
        var option = new S3StreamingMultipartUpload(
            S3StreamingMultipartUpload.DEFAULT_PART_SIZE,
            S3StreamingMultipartUpload.DEFAULT_MAX_IN_FLIGHT);

        // Write just under one part (less than 8 MiB) to test single-part upload on close
        int dataSize = 1024 * 1024; // 1 MiB
        byte[] expectedData = createDeterministicData(dataSize);

        try (var channel = new S3StreamingMultipartUploadChannel(s3Path, s3Client, option)) {
            channel.write(ByteBuffer.wrap(expectedData));
        }

        // Read back and verify
        byte[] actualData = readObject(key);
        assertThat(actualData).isEqualTo(expectedData);
    }

    @Test
    @DisplayName("force() persists data mid-stream")
    void force_persistsDataMidStream() throws Exception {
        var key = "force-test-object.bin";
        var s3Path = createS3Path(key);
        var option = new S3StreamingMultipartUpload(PART_SIZE, 4);

        // Write data and force to persist mid-stream
        int firstWriteSize = PART_SIZE + (PART_SIZE / 2); // 1.5 parts
        byte[] firstData = createDeterministicData(firstWriteSize);

        try (var channel = new S3StreamingMultipartUploadChannel(s3Path, s3Client, option)) {
            channel.write(ByteBuffer.wrap(firstData));

            // Force completes the current session
            channel.force();

            // Verify the object exists with the first data
            byte[] midStreamData = readObject(key);
            assertThat(midStreamData).isEqualTo(firstData);

            // Write more data (starts a new multipart session)
            int secondWriteSize = PART_SIZE + 1024; // slightly over 1 part
            byte[] secondData = createDeterministicData(secondWriteSize);
            channel.write(ByteBuffer.wrap(secondData));
        }

        // After close, the object should have the second write's data
        // (force() completed the first session, close() completes the second)
        int secondWriteSize = PART_SIZE + 1024;
        byte[] expectedFinalData = createDeterministicData(secondWriteSize);
        byte[] finalData = readObject(key);
        assertThat(finalData).isEqualTo(expectedFinalData);
    }

    // ---- Helper methods ----

    /**
     * Creates a deterministic byte array of the given size with a repeating pattern.
     */
    private byte[] createDeterministicData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 251); // Use prime number for varied pattern
        }
        return data;
    }

    /**
     * Creates an S3Path for the given key in the test bucket.
     */
    private S3Path createS3Path(String key) {
        var path = Paths.get(URI.create(
            Containers.localStackConnectionEndpoint() + "/" + bucketName + "/" + key));
        return (S3Path) path;
    }

    /**
     * Reads an object from S3 and returns its content as a byte array.
     */
    private byte[] readObject(String key) throws Exception {
        var request = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(key)
            .build();

        return s3Client.getObject(request, AsyncResponseTransformer.toBytes()).get().asByteArray();
    }
}
