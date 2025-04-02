/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import software.amazon.awssdk.crt.checksums.CRC32C;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class Crc32cFileIntegrityCheck extends S3ObjectIntegrityCheck {

    Crc32cFileIntegrityCheck() {
        super(new byte[16 * 1024], new CRC32C());
    }

    @Override
    protected void apply(PutObjectRequest.Builder putObjectRequest, Path file) {
        putObjectRequest.checksumAlgorithm(ChecksumAlgorithm.CRC32_C);
        int checksum = (int) calculateChecksum(file);
        putObjectRequest.checksumCRC32C(checksumToBase64String(checksum));
    }

    @Override
    public S3OpenOption newInstance() {
        return new Crc32cFileIntegrityCheck();
    }
}
