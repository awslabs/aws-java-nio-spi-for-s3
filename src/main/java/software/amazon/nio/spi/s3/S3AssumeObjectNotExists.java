/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Assume that the S3 object does not exist and therefore no download from S3 is needed and an accidental overwrite is
 * prevented while uploading to S3.
 *
 * <p>
 * This option sets an HTTP <code>If-None-Match</code> header with a wildcard value for the {@link PutObjectRequest},
 * which tells S3 to proceed with the upload only if the object doesn't already exist.
 *
 * <p>
 * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel}. Additionally, it
 * suppresses the download while opening the channel - no {@code GetObjectRequest} is involved. When closing the channel
 * the <code>If-None-Match</code> header is set to prevent overwriting an existing S3 object.
 *
 * <p>
 * Currently, this option in combination with <code>FileChannel.force</code> is unsupported. The reason is, that
 * <code>FileChannel.force</code> uploads to S3 and any subsequent <code>force</code> or <code>close</code> would fail
 * due to the <code>If-None-Match</code> header.
 */
class S3AssumeObjectNotExists extends S3OpenOption {

    static final S3AssumeObjectNotExists INSTANCE = new S3AssumeObjectNotExists();

    private S3AssumeObjectNotExists() {
    }

    @Override
    protected void apply(PutObjectRequest.Builder putObjectRequest, Path file) {
        putObjectRequest.ifNoneMatch("*");
    }

    @Override
    public S3OpenOption copy() {
        return INSTANCE;
    }
}
