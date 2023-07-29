/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.nio.spi.s3.S3ClientProvider;

/**
 *
 */
public class FixedS3ClientProvider extends S3ClientProvider {

    final public AwsClient client;

    public FixedS3ClientProvider(S3AsyncClient client) {
        this.client = client;
    }

    @Override
    public S3Client universalClient() {
        return (S3Client)client;
    }

    @Override
    protected S3AsyncClient generateAsyncClient(String bucketName) {
        return (S3AsyncClient)client;
    }

    @Override
    protected S3Client generateClient (String bucketName) {
        return (S3Client)client;
    }

}
