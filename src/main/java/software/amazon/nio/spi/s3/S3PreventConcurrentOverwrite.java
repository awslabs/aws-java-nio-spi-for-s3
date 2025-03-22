/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * Sets an HTTP <code>If-Match</code> header for the {@link PutObjectRequest} with the previously read ETag from the
 * {@link GetObjectResponse}.
 *
 * <p>
 * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel}. When opening the
 * channel, the ETag is stored. Closing the channel (or while calling <code>FileChannel.force</code>) the stored ETag is
 * set as <code>If-Match</code> header on the corresponding {@link PutObjectRequest}. This prevents overwriting of the
 * S3 object between opening and closing the channel.
 *
 * <p>
 * If multiple conditional writes occur for the same object name, the first write operation to finish succeeds. Amazon
 * S3 then fails subsequent writes with a <code>412 Precondition Failed</code> response.
 */
@NotThreadSafe
class S3PreventConcurrentOverwrite extends S3OpenOption {
    private String eTag;

    @Override
    protected void apply(PutObjectRequest.Builder putObjectRequest) {
        putObjectRequest.ifMatch(eTag);
    }

    @Override
    protected void consume(GetObjectResponse getObjectResponse) {
        eTag = getObjectResponse.eTag();
    }

    @Override
    protected void consume(PutObjectResponse putObjectResponse) {
        eTag = putObjectResponse.eTag();
    }
}
