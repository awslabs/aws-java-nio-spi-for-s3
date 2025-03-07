/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

enum DisabledFileIntegrityCheck implements S3ObjectIntegrityCheck {
    INSTANCE;

    @Override
    public void addChecksumToRequest(Path file, PutObjectRequest.Builder builder) {
        // nothing to do
    }
}
