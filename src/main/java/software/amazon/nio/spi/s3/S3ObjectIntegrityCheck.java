/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.Path;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Defines how to create a checksum to check the integrity of an object uploaded to S3.
 */
public interface S3ObjectIntegrityCheck {

    /**
     * Calculates the checksum for the specified file and adds it as a header to the PUT object request to be created.
     *
     * @param file
     *            the file to be used for creating the checksum
     * @param builder
     *            put object request
     */
    void addChecksumToRequest(Path file, PutObjectRequest.Builder builder);

}
