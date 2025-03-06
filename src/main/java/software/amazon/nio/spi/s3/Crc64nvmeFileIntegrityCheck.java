/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import software.amazon.awssdk.crt.checksums.CRC64NVME;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.internal.Base16;

class Crc64nvmeFileIntegrityCheck implements S3ObjectIntegrityCheck {
    private final byte[] buffer = new byte[16 * 1024];
    private final CRC64NVME checksum = new CRC64NVME();
    private final ByteBuffer checksumBuffer = ByteBuffer.allocate(Long.BYTES);

    @Override
    public void addChecksumToRequest(Path file, PutObjectRequest.Builder builder) {
        checksum.reset();
        checksumBuffer.clear();
        try (var in = Files.newInputStream(file)) {
            int len;
            while ((len = in.read(buffer)) != -1) {
                checksum.update(buffer, 0, len);
            }
            checksumBuffer.putLong(checksum.getValue());
            builder.checksumAlgorithm(ChecksumAlgorithm.CRC64_NVME);
            builder.checksumCRC64NVME(Base16.encodeAsString(checksumBuffer.array()));
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }
}
