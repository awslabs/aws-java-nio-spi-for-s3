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

class Crc64nvmeFileIntegrityCheckTest {

    @Test
    void test(@TempDir Path tempDir) throws IOException {
        var integrityCheck = new Crc64nvmeFileIntegrityCheck();
        var file = tempDir.resolve("test");
        Files.writeString(file, "hello world!", CREATE_NEW);
        var putObjectRequest = PutObjectRequest.builder();
        integrityCheck.addChecksumToRequest(file, putObjectRequest);
        assertThat(putObjectRequest.build().checksumCRC64NVME()).isEqualTo("D9160D1FA8E418E3");
    }

}
