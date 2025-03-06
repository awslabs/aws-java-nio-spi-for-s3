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

class DisabledFileIntegrityCheckTest {

    @Test
    void test(@TempDir Path tempDir) throws IOException {
        var integrityCheck = DisabledFileIntegrityCheck.INSTANCE;
        var file = tempDir.resolve("test");
        Files.writeString(file, "abc", CREATE_NEW);
        var putObjectRequest = PutObjectRequest.builder();
        integrityCheck.addChecksumToRequest(file, putObjectRequest);
        assertThat(putObjectRequest.build().checksumAlgorithmAsString()).isNull();
        assertThat(putObjectRequest.build().checksumCRC32()).isNull();
        assertThat(putObjectRequest.build().checksumCRC32C()).isNull();
        assertThat(putObjectRequest.build().checksumCRC64NVME()).isNull();
    }

}
