/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.nio.file.StandardOpenOption.*;
import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class Crc32cFileIntegrityCheckTest {

    @Test
    void test(@TempDir Path tempDir) throws IOException {
        var integrityCheck = new Crc32cFileIntegrityCheck();
        var file = tempDir.resolve("test");
        Files.writeString(file, "hello world!", CREATE_NEW);
        var putObjectRequest = PutObjectRequest.builder();
        integrityCheck.apply(putObjectRequest, file);
        assertThat(putObjectRequest.build().checksumCRC32C()).isEqualTo("SctXdw==");
    }

}
