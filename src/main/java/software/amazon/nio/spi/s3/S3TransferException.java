/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.nio.file.Path;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Represents an error which is caused by an S3 action and enables a caller to react based on the {@link #errorCode()}
 * or {@link #statusCode()}.
 *
 * <p>
 * It is meant to be used, to re-apply changes on a <code>FileChannel</code> or <code>SeekableByteChannel</code> when
 * the upload fails due to an expected failure like a failing precondition (caused by a conditional request). The caller
 * would catch the {@link S3TransferException} and checks for the {@link #errorCode()} or {@link #statusCode()}.
 * 
 * <p>
 * For example, when opening a FileChannel with the option {@link S3OpenOption#preventConcurrentOverwrite()} and the
 * upload fails while closing the channel due to a non-matching ETag, the caller catches the
 * {@link S3TransferException}, checks for the {@link #statusCode()} being equal to <code>412</code> performs the same
 * procedure again from the beginning.
 */
public class S3TransferException extends IOException {
    private static final long serialVersionUID = 1L;

    private final String errorCode;
    private final String errorMessage;
    private final Integer numAttempts;
    private final String requestId;
    private final int statusCode;

    public S3TransferException(String method, Path path, AwsServiceException cause) {
        this(
            errorCode(cause),
            errorMessage(cause),
            cause.requestId(),
            cause.statusCode(),
            cause.numAttempts(),
            message(method, path, cause),
            cause);
    }

    private S3TransferException(
        String errorCode,
        String errorMessage,
        String requestId,
        int statusCode,
        Integer numAttempts,
        String message,
        Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.requestId = requestId;
        this.statusCode = statusCode;
        this.numAttempts = numAttempts;
    }

    private static String errorCode(AwsServiceException cause) {
        return cause.awsErrorDetails() == null ? "" : cause.awsErrorDetails().errorCode();
    }

    private static String errorMessage(AwsServiceException cause) {
        return cause.awsErrorDetails() == null ? "" : cause.awsErrorDetails().errorMessage();
    }

    private static String message(String method, Path path, AwsServiceException cause) {
        String errorMessage = errorMessage(cause);
        errorMessage = errorMessage.isEmpty() ? "" : "; " + errorMessage;
        return method + " => " + cause.statusCode() + "; " + path + errorMessage;
    }

    public String errorCode() {
        return errorCode;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public Integer numAttempts() {
        return numAttempts;
    }

    public String requestId() {
        return requestId;
    }

    public int statusCode() {
        return statusCode;
    }
}
