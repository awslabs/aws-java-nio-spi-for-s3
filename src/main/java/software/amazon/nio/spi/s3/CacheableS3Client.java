/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import software.amazon.awssdk.services.s3.DelegatingS3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * A wrapper around an {@code S3AsyncClient} that can be used in a cache. It keeps track of the "closed" status of the wrapped
 * client and adds a {@code boolean isClosed()} method that can be used to determine if any client returned by the cache has
 * been previously closed. All other method calls are delegated to the wrapped client.
 */
public class CacheableS3Client extends DelegatingS3AsyncClient {

    private boolean closed = false;

    public CacheableS3Client(S3AsyncClient client) {
        super(client);
    }

    @Override
    public void close() {
        this.closed = true;
        super.close();
    }

    public boolean isClosed() {
        return closed;
    }
}
