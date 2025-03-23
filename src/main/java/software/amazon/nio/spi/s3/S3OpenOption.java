/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.OpenOption;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * Represents an S3 client specific {@link OpenOption} that enables the customization of the underlying
 * {@link GetObjectRequest}.
 */
public abstract class S3OpenOption implements OpenOption {

    /**
     * Sets an HTTP <code>If-Match</code> header for the {@link PutObjectRequest} with a previously read ETag from the
     * {@link GetObjectResponse}.
     *
     * <p>
     * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel}. When opening the
     * channel, the ETag is stored. Closing the channel (or while calling <code>FileChannel.force</code>) the stored
     * ETag is set as <code>If-Match</code> header on the corresponding {@link PutObjectRequest}. This prevents
     * overwriting of the S3 object between opening and closing the channel.
     *
     * <p>
     * If multiple conditional writes occur for the same object name, the first write operation to finish succeeds.
     * Amazon S3 then fails subsequent writes with a <code>412 Precondition Failed</code> response.
     */
    public static S3OpenOption preventConcurrentOverwrite() {
        return new S3PreventConcurrentOverwrite();
    }

    /**
     * Sets an HTTP <code>Range</code> header for a {@link GetObjectRequest}, e.g. <code>Range: bytes=0-100</code>.
     */
    public static S3OpenOption range(int end) {
        return new S3RangeHeader(0, end);
    }

    /**
     * Sets an HTTP <code>Range</code> header for a {@link GetObjectRequest}, e.g. <code>Range: bytes=50-100</code>.
     */
    public static S3OpenOption range(int start, int end) {
        return new S3RangeHeader(start, end);
    }

    /**
     * Adapts the given {@link GetObjectRequest.Builder}.
     *
     * @param getObjectRequest
     *            get object request
     */
    protected void apply(GetObjectRequest.Builder getObjectRequest) {
    }

    /**
     * Adapts the given {@link PutObjectRequest.Builder}.
     *
     * @param putObjectRequest
     *            put object request
     */
    protected void apply(PutObjectRequest.Builder putObjectRequest) {
    }

    /**
     * Will be called after the {@link GetObjectRequest} succeeded.
     *
     * @param getObjectResponse
     *            get object response
     */
    protected void consume(GetObjectResponse getObjectResponse) {
    }

    /**
     * Will be called after the {@link PutObjectResponse} succeeded.
     *
     * @param putObjectResponse
     *            put object response
     */
    protected void consume(PutObjectResponse putObjectResponse) {
    }
}
