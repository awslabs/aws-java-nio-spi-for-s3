/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * This open options prevents the {@link PutObjectRequest} when the content of the local temporary file has not changed.
 *
 * <p>
 * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel}. When opening the
 * channel, a checksum is calculated for the downloaded file and stored. When closing the channel (or while calling
 * <code>FileChannel.force</code>) a checksum is calculated again and compared to the previously stored one. If the
 * checksum matches, no {@link PutObjectRequest} is performed.
 */
@NotThreadSafe
class S3PutOnlyIfModified extends S3OpenOption {

    private final S3ObjectIntegrityCheck algorithm;
    private long checksum;

    S3PutOnlyIfModified(S3ObjectIntegrityCheck checksumAlgorithm) {
        this.algorithm = checksumAlgorithm;
    }

    @Override
    protected void consume(GetObjectResponse getObjectResponse, Path file) {
        checksum = algorithm.calculateChecksum(file);
    }

    @Override
    protected void consume(PutObjectResponse getObjectResponse, Path file) {
        checksum = algorithm.calculateChecksum(file);
    }

    @Override
    public S3OpenOption copy() {
        return new S3PutOnlyIfModified(algorithm);
    }

    @Override
    protected boolean preventPutObjectRequest(Path file) {
        return checksum == algorithm.calculateChecksum(file);
    }
}
