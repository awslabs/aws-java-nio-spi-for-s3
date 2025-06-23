/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;
import software.amazon.nio.spi.s3.util.LibraryVersion;

/**
 * Creates async S3 clients used by this library.
 */
public class S3ClientProvider {

    /**
     * Custom execution interceptor to add identifying headers to S3 requests
     */
    private static class S3NioSpiInterceptor implements ExecutionInterceptor {
        private static final String USER_AGENT_HEADER = "User-Agent";
        private static final String CLIENT_NAME_HEADER = "X-Amz-Client-Name";
        private static final String CLIENT_VERSION_HEADER = "X-Amz-Client-Version";
        
        private static final String LIBRARY_NAME = LibraryVersion.getLibraryName();
        private static final String LIBRARY_VERSION = LibraryVersion.getVersion();
        
        @Override
        public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
            return context.httpRequest().toBuilder()
                    .appendHeader(USER_AGENT_HEADER, LIBRARY_NAME + "/" + LIBRARY_VERSION)
                    .appendHeader(CLIENT_NAME_HEADER, LIBRARY_NAME)
                    .appendHeader(CLIENT_VERSION_HEADER, LIBRARY_VERSION)
                    .build();
        }
    }

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
                    
    /**
     * Flag to determine if we should use CRT client or regular client with custom headers
     */
    private boolean useCustomHeaders = false;


    private final Cache<String, CacheableS3Client> bucketClientCache = Caffeine.newBuilder()
            .maximumSize(4)
            .expireAfterWrite(Duration.ofHours(1))
            .build();

    public S3ClientProvider(S3NioSpiConfiguration c) {
        this.configuration = (c == null) ? new S3NioSpiConfiguration() : c;
        // Check system property to enable custom headers globally
        this.useCustomHeaders = Boolean.parseBoolean(
            System.getProperty("s3.spi.client.custom-headers.enabled", "false")
        );
    }

    public void asyncClientBuilder(final S3CrtAsyncClientBuilder builder) {
        asyncClientBuilder = builder;
    }
    
    /**
     * Enable or disable custom headers for client identification.
     * When enabled, uses regular S3AsyncClient instead of CRT client.
     * 
     * @param enabled true to enable custom headers, false to use CRT client
     */
    public void setCustomHeadersEnabled(boolean enabled) {
        this.useCustomHeaders = enabled;
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
            if (useCustomHeaders) {
                return bucketClientCache.get(bucket, b -> new CacheableS3Client(configureRegularClient().build()));
            } else {
                return bucketClientCache.get(bucket, b -> new CacheableS3Client(configureCrtClient().build()));
            }
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
    
    /**
     * Configure a regular S3AsyncClient with custom headers support
     */
    S3AsyncClientBuilder configureRegularClient() {
        var builder = S3AsyncClient.builder();
        
        var endpointUri = configuration.endpointUri();
        if (endpointUri != null) {
            builder.endpointOverride(endpointUri);
        }

        var credentials = configuration.getCredentials();
        if (credentials != null) {
            builder.credentialsProvider(() -> credentials);
        }

        var region = configuration.getRegion();
        if (region != null) {
            builder.region(Region.of(region));
        }

        // Add custom headers to identify requests from this library
        var clientOverrideConfig = ClientOverrideConfiguration.builder()
                .addExecutionInterceptor(new S3NioSpiInterceptor())
                .build();
        
        builder.overrideConfiguration(clientOverrideConfig);
        builder.forcePathStyle(configuration.getForcePathStyle());
        
        return builder;
    }

}
