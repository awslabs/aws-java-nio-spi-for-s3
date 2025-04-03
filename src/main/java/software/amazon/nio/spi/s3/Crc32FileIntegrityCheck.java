/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import software.amazon.awssdk.crt.checksums.CRC32;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class Crc32FileIntegrityCheck extends S3ObjectIntegrityCheck {

    Crc32FileIntegrityCheck() {
        super(new byte[16 * 1024], new CRC32());
    }

    @Override
    protected void apply(PutObjectRequest.Builder putObjectRequest, Path file) {
        putObjectRequest.checksumAlgorithm(ChecksumAlgorithm.CRC32);
        int checksum = (int) calculateChecksum(file);
        putObjectRequest.checksumCRC32(checksumToBase64String(checksum));
    }

    @Override
    public S3OpenOption copy() {
        return new Crc32FileIntegrityCheck();
    }
}
