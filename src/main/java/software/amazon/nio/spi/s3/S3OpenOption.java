/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.OpenOption;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Represents an S3 client specific {@link OpenOption} that enables the customization of the underlying
 * {@link GetObjectRequest}.
 */
public interface S3OpenOption extends OpenOption {

    static S3OpenOption range(int end) {
        return new S3RangeHeader(0, end);
    }

    static S3OpenOption range(int start, int end) {
        return new S3RangeHeader(start, end);
    }

    /**
     * Adapts the given {@link GetObjectRequest.Builder}.
     *
     * @param getObjectRequest
     *            get object request
     */
    void apply(GetObjectRequest.Builder getObjectRequest);
}
