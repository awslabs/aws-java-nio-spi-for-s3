/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
     * Assume that the S3 object does not exist and therefore no download from S3 is needed and an accidental overwrite
     * is prevented while uploading to S3.
     *
     * <p>
     * This option sets an HTTP <code>If-None-Match</code> header with a wildcard value for the
     * {@link PutObjectRequest}, which tells S3 to proceed with the upload only if the object doesn't already exist.
     *
     * <p>
     * This is meant to be used while opening a {@code FileChannel} or {@code SeekableByteChannel}. Additionally, it
     * suppresses the download while opening the channel - no {@code GetObjectRequest} is involved. When closing the
     * channel the <code>If-None-Match</code> header is set to prevent overwriting an existing S3 object.
     *
     * <p>
     * Currently, this option in combination with <code>FileChannel.force</code> is unsupported. The reason is, that
     * <code>FileChannel.force</code> uploads to S3 and any subsequent <code>force</code> or <code>close</code> would
     * fail due to the <code>If-None-Match</code> header.
     *
     * @return same instance
     */
    public static S3OpenOption assumeObjectNotExists() {
        return S3AssumeObjectNotExists.INSTANCE;
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
     * @return new instance
     */
    public static S3OpenOption putOnlyIfModified() {
        return new S3PutOnlyIfModified(new Crc32FileIntegrityCheck());
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
     * Removes all {@link S3OpenOption}s from the given set.
     *
     * @param options
     *            options that may contain {@link S3OpenOption}s
     * @return new set of filtered options
     */
    static Set<OpenOption> removeAll(Set<? extends OpenOption> options) {
        return options.stream()
            .filter(o -> !(o instanceof S3OpenOption))
            .collect(Collectors.toSet());
    }

    /**
     * Retains all {@link S3OpenOption}s from the given set.
     *
     * @param options
     *            options that may contain {@link S3OpenOption}s
     * @return new set of filtered options
     */
    static Set<S3OpenOption> retainAll(Set<? extends OpenOption> options) {
        return options.stream()
            .flatMap(o -> o instanceof S3OpenOption
                ? Stream.of((S3OpenOption) o)
                : Stream.empty())
            .collect(Collectors.toSet());
    }

    /**
     * Enables the usage of the {@code S3TransferManager}.
     *
     * <p>
     * This open options tells the implementation to use the {@code S3TransferManager} for {@code GetObjectRequest} or
     * {@code PutObjectRequest} requests if applicable. It is meant to be used while opening a {@code FileChannel} or
     * {@code SeekableByteChannel} on large S3 objects. For small objects (typically smaller than 8 MiB) the
     * {@code GetObjectRequest} or {@code PutObjectRequest} operations are sufficient.
     *
     * <p>
     * The {@code S3TransferManager} allows you to transfer a single object with enhanced throughput by leveraging
     * multi-part upload and byte-range fetches to perform transfers in parallel.
     *
     * @return same instance
     */
    public static S3OpenOption useTransferManager() {
        return S3UseTransferManager.INSTANCE;
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
     * Creates a new instance unless it is thread-safe.
     * 
     * @return new instance
     */
    public abstract S3OpenOption copy();

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
