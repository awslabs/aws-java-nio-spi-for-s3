/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

/**
 * This open options tells the implementation to use the {@code S3TransferManager} for {@code GetObjectRequest} or
 * {@code PutObjectRequest} requests if applicable. The {@code S3TransferManager} allows you to transfer a single object
 * with enhanced throughput by leveraging multi-part upload and byte-range fetches to perform transfers in parallel.
 *
 * <p>
 * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel} on large S3 objects. For
 * small objects (typically smaller than 8 MiB) the {@code GetObjectRequest} or {@code PutObjectRequest} operations are
 * sufficient.
 */
class S3UseTransferManager extends S3OpenOption {
    static final S3UseTransferManager INSTANCE = new S3UseTransferManager();

    private S3UseTransferManager() {
        // use INSTANCE
    }

    @Override
    public S3OpenOption copy() {
        return INSTANCE;
    }
}
