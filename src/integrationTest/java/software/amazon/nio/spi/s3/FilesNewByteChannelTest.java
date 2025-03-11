/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.*;
import static org.assertj.core.api.Assertions.*;
import static software.amazon.nio.spi.s3.Containers.*;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@DisplayName("Files$newByteChannel* should read and write on S3")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesNewByteChannelTest {

    String bucketName;

    @BeforeEach
    public void createBucket() {
        bucketName = "byte-channel-bucket" + System.currentTimeMillis();
        Containers.createBucket(bucketName);
    }

    @Test
    @DisplayName("newByteChannel with CREATE and WRITE is supported")
    public void newByteChannel_CREATE_WRITE() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-create-write-test.txt"));

        String text = "we test Files#newByteChannel";
        try (var channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(text.getBytes()));
        }

        assertThat(path).hasContent(text);
    }

    @Test
    @DisplayName("newByteChannel with READ and WRITE is supported")
    public void newByteChannel_READ_WRITE() throws IOException {
        var path = putObject(bucketName, "bc-read-write-test.txt", "xyz");

        String text = "abcdefhij";
        try (var channel = Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            // write
            channel.position(3);
            channel.write(ByteBuffer.wrap("def".getBytes()));
            channel.position(0);
            channel.write(ByteBuffer.wrap("abc".getBytes()));
            channel.position(6);
            channel.write(ByteBuffer.wrap("hij".getBytes()));

            // read
            var dst = ByteBuffer.allocate(text.getBytes().length);
            channel.position(0);
            channel.read(dst);

            // verify
            assertThat(dst.array()).isEqualTo(text.getBytes());
        }

        assertThat(path).hasContent(text);
    }

    @Test
    @DisplayName("newByteChannel with CRC32 integrity check")
    public void newByteChannel_withIntegrityCheck_CRC32() throws Exception {
        String text = "we test the integrity check when closing the byte channel";

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC32").execute(() -> {
            var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-integrity-check.txt"));
            try (var channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(text.getBytes()));
            }

            assertThat(path).hasContent(text);
        });
    }

    @Test
    @DisplayName("newByteChannel with CRC32C integrity check")
    public void newByteChannel_withIntegrityCheck_CRC32C() throws Exception {
        String text = "we test the integrity check when closing the byte channel";

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC32C").execute(() -> {
            var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-integrity-check.txt"));
            try (var channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(text.getBytes()));
            }

            assertThat(path).hasContent(text);
        });
    }

    @Test
    @DisplayName("newByteChannel with CRC64NVME integrity check")
    public void newByteChannel_withIntegrityCheck_CRC64NVME() throws Exception {
        String text = "we test the integrity check when closing the byte channel";

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC64NVME").execute(() -> {
            var path = (S3Path) Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/bc-integrity-check.txt"));
            try (var channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(text.getBytes()));
            }

            assertThat(path).hasContent(text);
        });
    }

    @Test
    @DisplayName("newByteChannel with invalid integrity check")
    public void newByteChannel_withIntegrityCheck_invalid() throws Exception {
        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "invalid").execute(() -> {
            var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/int-check-algo-test.txt"));
            assertThatThrownBy(() -> Files.newByteChannel(path)).hasMessage("unknown integrity check algorithm 'invalid'");
        });
    }

}
