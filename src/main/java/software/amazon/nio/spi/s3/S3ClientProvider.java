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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 *
 */
public class S3ClientProvider {

    /**
     * Default S3CrtAsyncClientBuilder
     */
    protected S3CrtAsyncClientBuilder asyncClientBuilder = S3AsyncClient.crtBuilder();


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
        final Set<Integer> RETRYABLE_STATUS_CODES = Stream.of(
                HttpStatusCode.INTERNAL_SERVER_ERROR,
                HttpStatusCode.BAD_GATEWAY,
                HttpStatusCode.SERVICE_UNAVAILABLE,
                HttpStatusCode.GATEWAY_TIMEOUT
        ).collect(Collectors.toSet());

        final Set<Class<? extends Exception>> RETRYABLE_EXCEPTIONS = Stream.of(
                RetryableException.class,
                IOException.class,
                ApiCallAttemptTimeoutException.class,
                ApiCallTimeoutException.class).collect(Collectors.toSet());

        retryCondition = OrRetryCondition.create(
                RetryOnStatusCodeCondition.create(RETRYABLE_STATUS_CODES),
                RetryOnExceptionsCondition.create(RETRYABLE_EXCEPTIONS),
                RetryOnClockSkewCondition.create(),
                RetryOnThrottlingCondition.create()
        );
    }

    Logger logger = LoggerFactory.getLogger("S3ClientStoreProvider");


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
     * @return a S3Client not bound to a region
     */
    public <T extends AwsClient> T universalClient(boolean async) {
        return (T)((async) ? DEFAULT_ASYNC_CLIENT : DEFAULT_CLIENT);
    }

    /**
     * Generate a client for the named bucket using a default client to determine the location of the named bucket
     * @param bucketName the named of the bucket to make the client for
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3Client generateClient(String bucketName){
        //
        // TODO: use generics like in universalClient()
        //
        return generateClient(bucketName, universalClient());
    }

    /**
     * Generate an asynchronous client for the named bucket using a default client to determine the location of the named bucket
     * @param bucketName the named of the bucket to make the client for
     * @return an asynchronous S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateAsyncClient(String bucketName){
        return generateAsyncClient(bucketName, DEFAULT_CLIENT);
    }

    /**
     *
     */
    protected S3Client generateClient (String bucketName, S3Client locationClient) {
        logger.debug("generating client for bucket: '{}'", bucketName);
        S3Client bucketSpecificClient = null;

        String bucketLocation = null;
        try {
            logger.debug("determining bucket location with getBucketLocation");
            bucketLocation = locationClient.getBucketLocation(builder -> builder.bucket(bucketName)).locationConstraintAsString();
            bucketSpecificClient = this.clientForRegion(bucketLocation);
        } catch (S3Exception e) {
            if(e.statusCode() == 403) {
                logger.debug("Cannot determine location of '{}' bucket directly. Attempting to obtain bucket location with headBucket operation", bucketName);
                try {
                    final HeadBucketResponse headBucketResponse = locationClient.headBucket(builder -> builder.bucket(bucketName));
                    bucketSpecificClient = this.clientForRegion(headBucketResponse.
                            sdkHttpResponse()
                            .firstMatchingHeader("x-amz-bucket-region")
                            .orElseThrow(() -> new NoSuchElementException("Head Bucket Response doesn't include the header 'x-amz-bucket-region'")));
                } catch (S3Exception e2) {
                    if (e2.statusCode() == 301) {
                        bucketSpecificClient = this.clientForRegion(e2.awsErrorDetails().
                                sdkHttpResponse()
                                .firstMatchingHeader("x-amz-bucket-region")
                                .orElseThrow(() -> new NoSuchElementException("Head Bucket Response doesn't include the header 'x-amz-bucket-region'")));
                    } else {
                        throw e2;
                    }
                }
            } else {
                throw e;
            }
        }

        //
        // if here, no S3 nor other client has been created yet and we do not
        // have a location; we'll let it figure out from the profile region
        //
        logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucketName);

        if (bucketSpecificClient == null) {
            logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucketName);
            bucketSpecificClient = S3Client.builder()
                    .overrideConfiguration(conf -> conf.retryPolicy(builder -> builder
                        .retryCondition(retryCondition)
                        .backoffStrategy(backoffStrategy)))
                    .build();
        }

        return bucketSpecificClient;
    }

    /**
     * Generate an asynchronous client for the named bucket using a default client to determine the location of the named client
     * @param bucketName the named of the bucket to make the client for
     * @param locationClient the client used to determine the location of the named bucket, recommend using DEFAULT_CLIENT
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateAsyncClient (String bucketName, S3Client locationClient) {
        logger.debug("generating asynchronous client for bucket: '{}'", bucketName);
        S3AsyncClient bucketSpecificClient = null;

        String bucketLocation = null;
        try {
            logger.debug("determining bucket location with getBucketLocation");
            bucketLocation = locationClient.getBucketLocation(builder -> builder.bucket(bucketName)).locationConstraintAsString();
            bucketSpecificClient = this.asyncClientForRegion(bucketLocation);
        } catch (S3Exception e) {
            if(e.statusCode() == 403) {
                logger.debug("Cannot determine location of '{}' bucket directly. Attempting to obtain bucket location with headBucket operation", bucketName);
                try {
                    final HeadBucketResponse headBucketResponse = locationClient.headBucket(builder -> builder.bucket(bucketName));
                    bucketSpecificClient = this.asyncClientForRegion(headBucketResponse.sdkHttpResponse()
                            .firstMatchingHeader("x-amz-bucket-region")
                            .orElseThrow(() -> new NoSuchElementException("Head Bucket Response doesn't include the header 'x-amz-bucket-region'")));
                } catch (S3Exception e2) {
                    if (e2.statusCode() == 301) {
                        bucketSpecificClient = this.asyncClientForRegion(e2
                                .awsErrorDetails()
                                .sdkHttpResponse()
                                .firstMatchingHeader("x-amz-bucket-region")
                                .orElseThrow(() -> new NoSuchElementException("Head Bucket Response doesn't include the header 'x-amz-bucket-region'"))
                        );
                    } else {
                        throw e2;
                    }
                }
            } else {
                throw e;
            }
        }

        //
        // if here, no S3 nor other client has been created yet and we do not
        // have a location; we'll let it figure out from the profile region
        //
        logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucketName);

        if (bucketSpecificClient == null) {
            logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucketName);
            bucketSpecificClient = S3AsyncClient.builder()
                    .overrideConfiguration(conf -> conf.retryPolicy(builder -> builder
                            .retryCondition(retryCondition)
                            .backoffStrategy(backoffStrategy)))
                    .build();
        }

        return bucketSpecificClient;
    }

    private S3Client clientForRegion(String region) {
        // It may be useful to further cache clients for regions although at some point clients for buckets may need to be
        // specialized beyond just region end points.
        logger.debug("bucket region is: '{}'", region);

        S3ClientBuilder clientBuilder =  S3Client.builder()
            .overrideConfiguration(conf -> conf.retryPolicy(builder -> builder
                    .retryCondition(retryCondition)
                    .backoffStrategy(backoffStrategy)));

        //
        // If no regionString is provided, the builder will try with the
        // profile's region setting
        //
        if ((region != null) && (!region.trim().equals(""))) {
            clientBuilder.region(Region.of(region));
        }
        return clientBuilder.build();
    }

    private S3AsyncClient asyncClientForRegion(String region){
        // It may be useful to further cache clients for regions although at some point clients for buckets may need to be
        // specialized beyond just region end points.
        logger.debug("bucket region is: '{}'", region);

        //
        // If no regionString is provided, the builder will try with the
        // profile's region setting
        //
        if ((region != null) && (!region.trim().equals(""))) {
            asyncClientBuilder.region(Region.of(region));
        }

        return asyncClientBuilder.build();
    }

}
