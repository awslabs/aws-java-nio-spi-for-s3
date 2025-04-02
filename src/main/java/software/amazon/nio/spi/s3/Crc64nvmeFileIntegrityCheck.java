/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import software.amazon.awssdk.crt.checksums.CRC64NVME;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class Crc64nvmeFileIntegrityCheck extends S3ObjectIntegrityCheck {

    Crc64nvmeFileIntegrityCheck() {
        super(new byte[16 * 1024], new CRC64NVME());
    }

    @Override
    protected void apply(PutObjectRequest.Builder putObjectRequest, Path file) {
        putObjectRequest.checksumAlgorithm(ChecksumAlgorithm.CRC64_NVME);
        long checksum = calculateChecksum(file);
        putObjectRequest.checksumCRC64NVME(checksumToBase64String(checksum));
    }

    @Override
    public S3OpenOption newInstance() {
        return new Crc64nvmeFileIntegrityCheck();
    }
}
