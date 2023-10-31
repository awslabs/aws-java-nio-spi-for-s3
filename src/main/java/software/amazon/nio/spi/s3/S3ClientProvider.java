/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnClockSkewCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnStatusCodeCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnThrottlingCondition;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 * Factory/builder class that creates sync and async S3 clients. It also provides
 * default clients that can be used for basic operations (e.g. bucket discovery).
 *
 */
public class S3ClientProvider {

    /**
     * Default S3CrtAsyncClientBuilder
     */
    protected S3CrtAsyncClientBuilder asyncClientBuilder = S3AsyncClient.crtBuilder();

    /**
     * Configuration
     */
    final protected S3NioSpiConfiguration configuration;

    /**
     * Default client using the "https://s3.us-east-1.amazonaws.com" endpoint
     */
    private static final S3Client DEFAULT_CLIENT = S3Client.builder()
            .endpointOverride(URI.create("https://s3.us-east-1.amazonaws.com"))
            .region(Region.US_EAST_1)
            .build();

    /**
     * Default asynchronous client using the "https://s3.us-east-1.amazonaws.com" endpoint
     */
    private static final S3AsyncClient DEFAULT_ASYNC_CLIENT = S3AsyncClient.builder()
            .endpointOverride(URI.create("https://s3.us-east-1.amazonaws.com"))
            .region(Region.US_EAST_1)
            .build();

    private final EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy.builder()
            .baseDelay(Duration.ofMillis(200L))
            .maxBackoffTime(Duration.ofSeconds(5L))
            .build();

    final RetryCondition retryCondition;

    {
        final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(
                HttpStatusCode.INTERNAL_SERVER_ERROR,
                HttpStatusCode.BAD_GATEWAY,
                HttpStatusCode.SERVICE_UNAVAILABLE,
                HttpStatusCode.GATEWAY_TIMEOUT
        );

        final Set<Class<? extends Exception>> RETRYABLE_EXCEPTIONS = Set.of(
                RetryableException.class,
                IOException.class,
                ApiCallAttemptTimeoutException.class,
                ApiCallTimeoutException.class);

        retryCondition = OrRetryCondition.create(
                RetryOnStatusCodeCondition.create(RETRYABLE_STATUS_CODES),
                RetryOnExceptionsCondition.create(RETRYABLE_EXCEPTIONS),
                RetryOnClockSkewCondition.create(),
                RetryOnThrottlingCondition.create()
        );
    }

    static private final Logger logger = LoggerFactory.getLogger(S3ClientProvider.class);

    public S3ClientProvider(S3NioSpiConfiguration c) {
        this.configuration = (c == null) ? new S3NioSpiConfiguration() : c;
    }

    public S3ClientProvider() {
        this(null);
    }

    public S3CrtAsyncClientBuilder asyncClientBuilder() {
        return asyncClientBuilder;
    }

    public void asyncClientBuilder(final S3CrtAsyncClientBuilder builder) {
        asyncClientBuilder = builder;
    }


    /**
     * This method returns a universal client (i.e. not bound to any region)
     * that can be used by certain S3 operations for discovery.
     * This is the same as universalClient(false);
     *
     * @return a S3Client not bound to a region
     */
    public S3Client universalClient() {
        return universalClient(false);
    }

    /**
     * This method returns a universal client (i.e.not bound to any region)
     * that can be used by certain S3 operations for discovery
     *
     * @param async true to return an asynchronous client, false otherwise
     * @param <T> type of AwsClient
     * @return a S3Client not bound to a region
     */
    public <T extends AwsClient> T universalClient(boolean async) {
        return (T)((async) ? DEFAULT_ASYNC_CLIENT : DEFAULT_CLIENT);
    }

    /**
     * Generates a sync client for the named bucket using the provided location
     * discovery client.
     *
     * @param bucket the named of the bucket to make the client for
     *
     * @return an S3 client appropriate for the region of the named bucket
     *
     */
    protected S3AsyncClient generateAsyncClient(String bucket) {
        return generateAsyncClient(bucket,  universalClient());
    }

    /**
     * Generate a client for the named bucket using a provided client to
     * determine the location of the named client
     *
     * @param bucketName the name of the bucket to make the client for
     * @param locationClient the client used to determine the location of the
     *        named bucket, recommend using DEFAULT_CLIENT
     *
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3Client generateSyncClient(String bucketName, S3Client locationClient) {
        return getClientForBucket(bucketName, locationClient, this::clientForRegion);
    }

    /**
     * Generate an async  client for the named bucket using a provided client to
     * determine the location of the named client
     *
     * @param bucketName the name of the bucket to make the client for
     * @param locationClient the client used to determine the location of the
     *        named bucket, recommend using DEFAULT_CLIENT
     *
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateAsyncClient (String bucketName, S3Client locationClient) {
        return getClientForBucket(bucketName, locationClient, this::asyncClientForRegion);
    }

    protected <T extends AwsClient> T getClientForBucket(
            String bucketName,
            S3Client locationClient,
            Function<String, T> getClientForRegion
    ) {
        logger.debug("generating client for bucket: '{}'", bucketName);
        T bucketSpecificClient = null;

        if ((configuration.getEndpoint() == null) || configuration.getEndpoint().isBlank()) {
            //
            // we try to locate a bucket only if no endpoint is provided, which
            // means we are dealing with AWS S3 buckets
            //
            String bucketLocation = determineBucketLocation(bucketName, locationClient);

            if ( bucketLocation != null) {
                bucketSpecificClient = getClientForRegion.apply(bucketLocation);
            } else {
                // if here, no S3 nor other client has been created yet, and we do not
                // have a location; we'll let it figure out from the profile region
                logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucketName);
            }
        }

        return (bucketSpecificClient != null)
                ? bucketSpecificClient
                : getClientForRegion.apply(configuration.getRegion());
    }

    private String determineBucketLocation(String bucketName, S3Client locationClient) {
        try {
            return getBucketLocation(bucketName, locationClient);
        } catch (S3Exception e) {
            if(isForbidden(e)) {
                return getBucketLocationFromHead(bucketName, locationClient);
            } else {
                throw e;
            }
        }
    }

    private String getBucketLocation(String bucketName, S3Client locationClient) {
        logger.debug("determining bucket location with getBucketLocation");
        return locationClient.getBucketLocation(builder -> builder.bucket(bucketName)).locationConstraintAsString();
    }

    private String getBucketLocationFromHead(String bucketName, S3Client locationClient) {
        try {
            logger.debug("Cannot determine location of '{}' bucket directly. Attempting to obtain bucket location with headBucket operation", bucketName);
            final HeadBucketResponse headBucketResponse = locationClient.headBucket(builder -> builder.bucket(bucketName));
            return getBucketRegionFromResponse(headBucketResponse.sdkHttpResponse());
        } catch (S3Exception e) {
            if (isRedirect(e)) {
                return getBucketRegionFromResponse(e.awsErrorDetails().sdkHttpResponse());
            } else {
                throw e;
            }
        }
    }

    private boolean isForbidden(S3Exception e) { return e.statusCode() == 403; }
    private boolean isRedirect(S3Exception e) { return e.statusCode() == 301; }

    private String getBucketRegionFromResponse(SdkHttpResponse response) {
        return response.firstMatchingHeader("x-amz-bucket-region").orElseThrow(() ->
            new NoSuchElementException("Head Bucket Response doesn't include the header 'x-amz-bucket-region'")
        );
    }

    private S3Client clientForRegion(String regionName) {
        String endpoint = configuration.getEndpoint();
        AwsCredentials credentials = configuration.getCredentials();
        Region region = ((regionName == null) || (regionName.trim().isEmpty())) ? Region.US_EAST_1 : Region.of(regionName);

        logger.debug("bucket region is: '{}'", region.id());

        S3ClientBuilder clientBuilder =  S3Client.builder()
            .forcePathStyle(configuration.getForcePathStyle())
            .region(region)
            .overrideConfiguration(
                conf -> conf.retryPolicy(
                    builder -> builder.retryCondition(retryCondition).backoffStrategy(backoffStrategy)
                )
            );
        
        if (!endpoint.isBlank()) {
            clientBuilder.endpointOverride(URI.create(configuration.getEndpointProtocol() + "://" + endpoint));
        }

        if (credentials != null) {
            clientBuilder.credentialsProvider(() -> credentials);
        }

        return clientBuilder.build();
    }

    private S3AsyncClient asyncClientForRegion(String regionName) {
        String endpoint = configuration.getEndpoint();
        AwsCredentials credentials = configuration.getCredentials();

        Region region = ((regionName == null) || (regionName.trim().isEmpty())) ? Region.US_EAST_1 : Region.of(regionName);

        logger.debug("bucket region is: '{}'", region.id());

        if (!endpoint.isBlank()) {
            asyncClientBuilder.endpointOverride(URI.create(configuration.getEndpointProtocol() + "://" + endpoint));
        }

        if (credentials != null) {
            asyncClientBuilder.credentialsProvider(() -> credentials);
        }

        return asyncClientBuilder.forcePathStyle(configuration.getForcePathStyle()).region(region).build();
    }
}
