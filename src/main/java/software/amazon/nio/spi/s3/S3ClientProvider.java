/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 * Creates async S3 clients used by this library.
 */
public class S3ClientProvider {

    /**
     * Default asynchronous client using the "<a href="https://s3.us-east-1.amazonaws.com">...</a>" endpoint
     */
    @Deprecated
    protected S3AsyncClient universalClient;

    /**
     * Configuration
     */
    protected final S3NioSpiConfiguration configuration;

    /**
     * Default S3CrtAsyncClientBuilder
     */
    protected S3CrtAsyncClientBuilder asyncClientBuilder =
            S3AsyncClient.crtBuilder()
                    .crossRegionAccessEnabled(true);


    private final Cache<String, CacheableS3Client> bucketClientCache = Caffeine.newBuilder()
            .maximumSize(4)
            .expireAfterWrite(Duration.ofHours(1))
            .build();

    public S3ClientProvider(S3NioSpiConfiguration c) {
        this.configuration = (c == null) ? new S3NioSpiConfiguration() : c;
    }

    public void asyncClientBuilder(final S3CrtAsyncClientBuilder builder) {
        asyncClientBuilder = builder;
    }


    /**
     * Generates a sync client for the named bucket using a client configured by the default region configuration chain.
     *
     * @param bucket the named of the bucket to make the client for
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateClient(String bucket) {
        var client = bucketClientCache.getIfPresent(bucket);
        if (client != null && !client.isClosed()) {
            return client;
        } else {
            if (client != null && client.isClosed()) {
                bucketClientCache.invalidate(bucket);    // remove the closed client from the cache
            }
            return bucketClientCache.get(bucket, b -> new CacheableS3Client(configureCrtClient().build()));
        }
    }

    S3CrtAsyncClientBuilder configureCrtClient() {
        var endpointUri = configuration.endpointUri();
        if (endpointUri != null) {
            asyncClientBuilder.endpointOverride(endpointUri);
        }

        var credentials = configuration.getCredentials();
        if (credentials != null) {
            asyncClientBuilder.credentialsProvider(() -> credentials);
        }

        var region = configuration.getRegion();
        if (region != null) {
            asyncClientBuilder.region(Region.of(region));
        }

        return asyncClientBuilder.forcePathStyle(configuration.getForcePathStyle());
    }

}
