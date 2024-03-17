/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Utility class that extends {@code S3ClientProvider} with a given
 * implementation of S3AsyncClient. This may be helpful in tests or in use cases
 * where a single instance of a S3 client should be used.
 */
public class FixedS3ClientProvider extends S3ClientProvider {

    final public S3AsyncClient client;

    public FixedS3ClientProvider(S3AsyncClient client) {
        super(null);
        this.client = client;
    }

    @Override
    S3AsyncClient universalClient() {
        return client;
    }

    @Override
    protected S3AsyncClient generateClient(String bucketName) {
        return client;
    }

}
