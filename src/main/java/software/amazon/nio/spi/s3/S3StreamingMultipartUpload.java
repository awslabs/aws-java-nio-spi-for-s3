/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

/**
 * An {@link S3OpenOption} that activates streaming multipart upload behavior.
 *
 * <p>
 * When this option is included in the open options set, the channel will upload parts to S3 incrementally as data is
 * written, rather than buffering all data to a local temporary file. This reduces memory/disk pressure and provides
 * incremental upload progress for large objects.
 *
 * <p>
 * This option requires the AWS CRT client to be in use. The channel operates in append-only mode by default; if a
 * backward seek is detected, it falls back to the existing temp-file approach.
 *
 * @see S3OpenOption#streamingMultipartUpload()
 * @see S3OpenOption#streamingMultipartUpload(long)
 */
class S3StreamingMultipartUpload extends S3OpenOption {

    /**
     * Minimum part size allowed by S3: 5 MiB.
     */
    static final long MIN_PART_SIZE = 5 * 1024 * 1024;

    /**
     * Maximum part size allowed by S3: 5 GiB.
     */
    static final long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024;

    /**
     * Default part size: 8 MiB.
     */
    static final long DEFAULT_PART_SIZE = 8 * 1024 * 1024;

    /**
     * Maximum number of parts allowed by S3 per multipart upload.
     */
    static final int MAX_PARTS = 10_000;

    /**
     * Default maximum number of concurrent in-flight part uploads.
     */
    static final int DEFAULT_MAX_IN_FLIGHT = 4;

    private final long partSize;
    private final int maxInFlight;
    private final boolean fallbackEnabled;

    S3StreamingMultipartUpload(long partSize, int maxInFlight) {
        this(partSize, maxInFlight, false);
    }

    S3StreamingMultipartUpload(long partSize, int maxInFlight, boolean fallbackEnabled) {
        this.partSize = partSize;
        this.maxInFlight = maxInFlight;
        this.fallbackEnabled = fallbackEnabled;
    }

    /**
     * Returns the configured part size in bytes.
     *
     * @return the part size
     */
    long getPartSize() {
        return partSize;
    }

    /**
     * Returns the configured maximum number of concurrent in-flight part uploads.
     *
     * @return the max in-flight count
     */
    int getMaxInFlight() {
        return maxInFlight;
    }

    /**
     * Returns whether fallback to temp-file mode is enabled on non-sequential position changes.
     * When disabled, seeks throw {@link UnsupportedOperationException} and part data is not retained in memory.
     *
     * @return true if fallback is enabled (default), false if disabled for lower memory usage
     */
    boolean isFallbackEnabled() {
        return fallbackEnabled;
    }

    @Override
    public S3OpenOption copy() {
        return new S3StreamingMultipartUpload(partSize, maxInFlight, fallbackEnabled);
    }
}
