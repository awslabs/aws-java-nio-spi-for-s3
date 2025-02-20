/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.*;
import static software.amazon.nio.spi.s3.Containers.*;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@DisplayName("FileChannel$open* should read and write on S3")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileChannelTest {

    String bucketName;

    @BeforeEach
    public void createBucket() {
        bucketName = "file-channel-bucket" + System.currentTimeMillis();
        Containers.createBucket(bucketName);
    }

    @Test
    @DisplayName("open with READ and WRITE is supported")
    public void FileChannel_open() throws IOException {
        var path = putObject(bucketName, "fc-read-write-test.txt", "xyz");

        String text = "abcdefhij";
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            // write
            channel.write(ByteBuffer.wrap("def".getBytes()), 3);
            channel.write(ByteBuffer.wrap("abc".getBytes()), 0);
            channel.write(ByteBuffer.wrap("hij".getBytes()), 6);

            // read
            var dst = ByteBuffer.allocate(text.getBytes().length);
            channel.read(dst, 0);

            // verify
            assertThat(dst.array()).isEqualTo(text.getBytes());
        }

        assertThat(path).hasContent(text);
    }

    @Test
    @DisplayName("open with CREATE and WRITE is supported")
    public void FileChannel_open_CREATE() throws IOException {
        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/" + bucketName + "/fc-create-write-test.txt"));

        String text = "we test FileChannel#open";
        try (var channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(text.getBytes()));
        }

        assertThat(path).hasContent(text);
    }

}
