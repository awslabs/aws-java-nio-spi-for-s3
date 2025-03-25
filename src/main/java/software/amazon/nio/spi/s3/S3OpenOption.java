/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
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
     * Removes all {@link S3OpenOption}s from the given set.
     *
     * @param options
     *            options that may contain {@link S3OpenOption}s
     * @return new set of filtered options
     */
    static Set<? extends OpenOption> exclude(Set<? extends OpenOption> options) {
        return options.stream()
            .filter(o -> !(o instanceof S3OpenOption))
            .collect(Collectors.toSet());
    }

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
     *
     * @return new instance
     */
    public static S3OpenOption preventConcurrentOverwrite() {
        return new S3PreventConcurrentOverwrite();
    }

    /**
     * This open options prevents the {@link PutObjectRequest} when the content of the local temporary file has not
     * changed.
     *
     * <p>
     * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel}. When opening the
     * channel, a checksum is calculated for the downloaded file and stored. When closing the channel (or while calling
     * <code>FileChannel.force</code>) a checksum is calculated again and compared to the previously stored one. If the
     * checksum matches, no {@link PutObjectRequest} is performed.
     *
     * @param checksumAlgorithm
     *            the algorithm to calculate a checksum
     * @return new instance
     */
    public static S3OpenOption putOnlyIfModified(S3ObjectIntegrityCheck checksumAlgorithm) {
        return new S3PutOnlyIfModified(checksumAlgorithm);
    }

    /**
     * This open options prevents the {@link PutObjectRequest} when the content of the local temporary file has not
     * changed.
     *
     * <p>
     * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel}. When opening the
     * channel, a checksum is calculated for the downloaded file and stored. When closing the channel (or while calling
     * <code>FileChannel.force</code>) a checksum is calculated again and compared to the previously stored one. If the
     * checksum matches, no {@link PutObjectRequest} is performed.
     *
     * @return new instance
     */
    public static S3OpenOption putOnlyIfModified() {
        return new S3PutOnlyIfModified(new Crc32FileIntegrityCheck());
    }

    /**
     * Sets an HTTP <code>Range</code> header for a {@link GetObjectRequest}, e.g. <code>Range: bytes=0-100</code>.
     *
     * @param end
     *            exclusive end
     * @return new instance
     */
    public static S3OpenOption range(int end) {
        return new S3RangeHeader(0, end);
    }

    /**
     * Sets an HTTP <code>Range</code> header for a {@link GetObjectRequest}, e.g. <code>Range: bytes=50-100</code>.
     *
     * @param start
     *            first position (offset)
     * @param end
     *            last position (exclusive)
     * @return new instance
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
     * @param file
     *            file in which the content is saved locally
     */
    protected void apply(PutObjectRequest.Builder putObjectRequest, Path file) {
    }

    /**
     * Will be called after the {@link GetObjectRequest} succeeded.
     *
     * @param getObjectResponse
     *            get object response
     * @param file
     *            file in which the content is saved locally
     */
    protected void consume(GetObjectResponse getObjectResponse, Path file) {
    }

    /**
     * Will be called after the {@link PutObjectResponse} succeeded.
     *
     * @param putObjectResponse
     *            put object response
     * @param file
     *            file in which the content is saved locally
     */
    protected void consume(PutObjectResponse putObjectResponse, Path file) {
    }

    /**
     * Whether the {@link PutObjectRequest} should not be made.
     *
     * @param file
     *            file in which the content is saved locally
     * @return <code>true</code> if the upload should not be performed
     */
    protected boolean preventPutObjectRequest(Path file) {
        return false;
    }
}
