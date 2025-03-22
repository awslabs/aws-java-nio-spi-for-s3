/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Sets a HTTP <code>Range</code> header for a {@link GetObjectRequest}.
 */
class S3RangeHeader extends S3OpenOption {
    private final String range;

    S3RangeHeader(int start, int end) {
        if (start < 0) {
            throw new IllegalArgumentException("start must be nonnegative");
        }
        if (end < 0) {
            throw new IllegalArgumentException("end must be nonnegative");
        }
        range = "bytes=" + start + "-" + end;
    }

    @Override
    public void apply(GetObjectRequest.Builder builder) {
        builder.range(range);
    }
}
