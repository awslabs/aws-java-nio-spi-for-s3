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
import software.amazon.awssdk.crt.checksums.CRC32;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.BinaryUtils;

class Crc32FileIntegrityCheck implements S3ObjectIntegrityCheck {
    private final byte[] buffer = new byte[16 * 1024];
    private final CRC32 checksum = new CRC32();
    private final ByteBuffer checksumBuffer = ByteBuffer.allocate(Integer.BYTES);

    @Override
    public void addChecksumToRequest(Path file, PutObjectRequest.Builder builder) {
        checksum.reset();
        checksumBuffer.clear();
        try (var in = Files.newInputStream(file)) {
            int len;
            while ((len = in.read(buffer)) != -1) {
                checksum.update(buffer, 0, len);
            }
            checksumBuffer.putInt((int) checksum.getValue());
            builder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
            builder.checksumCRC32(BinaryUtils.toBase64(checksumBuffer.array()));
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }
}
