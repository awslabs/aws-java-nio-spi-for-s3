/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.util.concurrent.TimeUnit.MINUTES;
import static software.amazon.nio.spi.s3.util.TimeOutUtils.logAndGenerateExceptionOnTimeOut;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 * Factory/builder class that creates async S3 clients. It also provides
 * default clients that can be used for basic operations (e.g. bucket discovery).
 */
public class S3ClientProvider {

    private static final Logger logger = LoggerFactory.getLogger(S3ClientProvider.class);

    /**
     * Default asynchronous client using the "<a href="https://s3.us-east-1.amazonaws.com">...</a>" endpoint
     */
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

    private final Cache<String, String> bucketRegionCache = Caffeine.newBuilder()
            .maximumSize(16)
            .expireAfterWrite(Duration.ofMinutes(30))
            .build();

    private final Cache<String, CacheableS3Client> bucketClientCache = Caffeine.newBuilder()
            .maximumSize(4)
            .expireAfterWrite(Duration.ofHours(1))
            .build();

    public S3ClientProvider(S3NioSpiConfiguration c) {
        this.configuration = (c == null) ? new S3NioSpiConfiguration() : c;
        this.universalClient = S3AsyncClient.builder()
                .endpointOverride(URI.create("https://s3.us-east-1.amazonaws.com"))
                .crossRegionAccessEnabled(true)
                .region(Region.US_EAST_1)
                .build();
    }

    public void asyncClientBuilder(final S3CrtAsyncClientBuilder builder) {
        asyncClientBuilder = builder;
    }

    /**
     * This method returns a universal client bound to the us-east-1 region
     * that can be used by certain S3 operations for discovery such as getBucketLocation.
     *
     * @return an S3AsyncClient bound to us-east-1
     */
    S3AsyncClient universalClient() {
        return universalClient;
    }

    /**
     * Sets the fallback client used to make {@code S3AsyncClient#getBucketLocation()} calls. Typically, this would
     * only be set for testing purposes to use a {@code Mock} or {@code Spy} class.
     * @param client the client to be used for getBucketLocation calls.
     */
    void universalClient(S3AsyncClient client) {
        this.universalClient = client;
    }

    /**
     * Generates a sync client for the named bucket using a client configured by the default region configuration chain.
     *
     * @param bucket the named of the bucket to make the client for
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateClient(String bucket) {
        try {
            return generateClient(bucket, S3AsyncClient.create());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Generate an async client for the named bucket using a provided client to
     * determine the location of the named client
     *
     * @param bucketName     the name of the bucket to make the client for
     * @param locationClient the client used to determine the location of the
     *                       named bucket, recommend using {@code S3ClientProvider#UNIVERSAL_CLIENT}
     * @return an S3 client appropriate for the region of the named bucket
     */
    S3AsyncClient generateClient(String bucketName, S3AsyncClient locationClient)
            throws ExecutionException, InterruptedException {
        logger.debug("generating client for bucket: '{}'", bucketName);

        String bucketLocation = null;
        if (configuration.endpointUri() == null) {
            // we try to locate a bucket only if no endpoint is provided, which means we are dealing with AWS S3 buckets
            bucketLocation = getBucketLocation(bucketName, locationClient);

            if (bucketLocation == null) {
                // if here, no S3 nor other client has been created yet, and we do not
                // have a location; we'll let it figure out from the profile region
                logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.",
                    bucketName);
            }
        }

        var client = bucketClientCache.getIfPresent(bucketName);
        if (client != null && !client.isClosed()) {
            return client;
        } else {
            if (client != null && client.isClosed()) {
                bucketClientCache.invalidate(bucketName);    // remove the closed client from the cache
            }
            String r = Optional.ofNullable(bucketLocation).orElse(configuration.getRegion());
            return bucketClientCache.get(bucketName, b -> new CacheableS3Client(configureCrtClientForRegion(r)));
        }
    }

    private String getBucketLocation(String bucketName, S3AsyncClient locationClient)
            throws ExecutionException, InterruptedException {

        if (bucketRegionCache.getIfPresent(bucketName) != null) {
            return bucketRegionCache.getIfPresent(bucketName);
        }

        logger.debug("checking if the bucket is in the same region as the providedClient using HeadBucket");
        try (var client = locationClient) {
            final HeadBucketResponse response = client
                    .headBucket(builder -> builder.bucket(bucketName))
                    .get(configuration.getTimeoutLow(), MINUTES);
            bucketRegionCache.put(bucketName, response.bucketRegion());
            return response.bucketRegion();

        } catch (TimeoutException e) {
            throw logAndGenerateExceptionOnTimeOut(
                    logger,
                    "generateClient",
                    configuration.getTimeoutLow(),
                    MINUTES);
        } catch (Throwable t) {

            if (t instanceof ExecutionException &&
                    t.getCause() instanceof S3Exception &&
                    ((S3Exception) t.getCause()).statusCode() == 301) {  // you got a redirect, the region should be in the header
                logger.debug("HeadBucket was unsuccessful, redirect received, attempting to extract x-amz-bucket-region header");
                S3Exception s3e = (S3Exception) t.getCause();
                final var matchingHeaders = s3e.awsErrorDetails().sdkHttpResponse().matchingHeaders("x-amz-bucket-region");
                if (matchingHeaders != null && !matchingHeaders.isEmpty()) {
                    bucketRegionCache.put(bucketName, matchingHeaders.get(0));
                    return matchingHeaders.get(0);
                }
            } else if (t instanceof ExecutionException &&
                    t.getCause() instanceof S3Exception &&
                    ((S3Exception) t.getCause()).statusCode() == 403) {  // HeadBucket was forbidden
                logger.debug("HeadBucket forbidden. Attempting a call to GetBucketLocation using the UNIVERSAL_CLIENT");
                try {
                    String location =  universalClient.getBucketLocation(builder -> builder.bucket(bucketName))
                            .get(configuration.getTimeoutLow(), MINUTES).locationConstraintAsString();
                    bucketRegionCache.put(bucketName, location);
                } catch (TimeoutException e) {
                    throw logAndGenerateExceptionOnTimeOut(
                            logger,
                            "generateClient",
                            configuration.getTimeoutLow(),
                            MINUTES);
                }
            } else {
                // didn't handle the exception - rethrow it
                throw t;
            }
        }
        return "";
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

        return asyncClientBuilder.forcePathStyle(configuration.getForcePathStyle());
    }

    private S3AsyncClient configureCrtClientForRegion(String regionName) {
        var region = getRegionFromRegionName(regionName);
        logger.debug("bucket region is: '{}'", region);
        return configureCrtClient().region(region).build();
    }

    private static Region getRegionFromRegionName(String regionName) {
        return (regionName == null || regionName.isBlank()) ? null : Region.of(regionName);
    }

}
