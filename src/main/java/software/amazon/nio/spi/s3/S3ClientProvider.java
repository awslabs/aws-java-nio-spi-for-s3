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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 * TODO: remove credentials as they can now be constructed from configuration
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

    public S3ClientProvider(S3NioSpiConfiguration c) {
        this.configuration = (c == null) ? new S3NioSpiConfiguration() : c;
    }

    public S3ClientProvider() {
        this(null);
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
     * @return a S3Client not bound to a region
     */
    public <T extends AwsClient> T universalClient(boolean async) {
        return (T)((async) ? DEFAULT_ASYNC_CLIENT : DEFAULT_CLIENT);
    }

    /**
     * Same as generateClient(null, bucketName, null, universalClient())
     */
    protected S3Client generateClient(String bucketName){
        //
        // TODO: use generics like in universalClient()
        //
        return this.generateClient(null, bucketName, null, universalClient());
    }

    /**
     * Same as generateAsyncClient(null, bucketName, null)
     */
    protected S3AsyncClient generateAsyncClient(String bucketName){
        //
        // TODO: use generics like in universalClient()
        //
        return this.generateAsyncClient(null, bucketName, null);
    }

    /**
     * Same as generateAsyncClient(endpoint, bucketName, credentials, universalClient())
     */
    protected S3AsyncClient generateAsyncClient(String endpoint, String bucket, AwsCredentials credentials) {
        return generateAsyncClient(endpoint, bucket,  credentials, universalClient());
    }

    /**
     * Same as generateClient(endpoint, bucketName, universalClient())
     */
    protected S3Client generateClient (String endpoint, String bucket, AwsCredentials credentials) {
        return generateClient(endpoint, bucket,  credentials, universalClient());
    }

    /**
     * Same as generateAsyncClient(null, bucketName, null, locationClient)
     */
    protected S3AsyncClient generateAsyncClient (String bucket, S3Client locationClient) {
        return generateAsyncClient(null, bucket,  null, locationClient);
    }

    /**
     * Same as generateClient(null, bucketName, null, locationClient)
     */
    protected S3Client generateClient (String bucket, S3Client locationClient) {
        return generateClient(null, bucket, null, locationClient);
    }

    /**
     * Same as generateClient(endpoint, bucketName, universalClient())
     */
    protected S3Client generateClient (String endpoint, String bucket, AwsCredentials credentials, S3Client locationClient) {
        logger.debug("generating client for bucket: '{}'", bucket);
        S3Client bucketSpecificClient = null;

        if (endpoint == null) {
            String bucketLocation = null;
            try {
                logger.debug("determining bucket location with getBucketLocation");
                bucketLocation = locationClient.getBucketLocation(builder -> builder.bucket(bucket)).locationConstraintAsString();
                bucketSpecificClient = this.clientForRegion(bucketLocation);
            } catch (S3Exception e) {
                if(e.statusCode() == 403) {
                    logger.debug("Cannot determine location of '{}' bucket directly. Attempting to obtain bucket location with headBucket operation", bucket);
                    try {
                        final HeadBucketResponse headBucketResponse = locationClient.headBucket(builder -> builder.bucket(bucket));
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
        }

        //
        // if here, no S3 nor other client has been created yet and we do not
        // have a location; we'll let it figure out from the profile region
        //

        logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucket);
        return (bucketSpecificClient != null) ? bucketSpecificClient : clientForRegion(endpoint, bucket, null, credentials);
    }

    /**
     * Generate an asynchronous client for the named bucket using a default client to determine the location of the named client
     * @param endpoint the endpoint to make the client for
     * @param bucket the named of the bucket to make the client for
     * @param credentials the credentials to use to make the client - can be null
     * @param locationClient the client used to determine the location of the named bucket, recommend using DEFAULT_CLIENT
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateAsyncClient (String endpoint, String bucket, AwsCredentials credentials, S3Client locationClient) {
        logger.debug("generating asynchronous client for bucket: '{}'", bucket);
        S3AsyncClient bucketSpecificClient = null;

        if (endpoint == null) {
            String bucketLocation = null;
            try {
                logger.debug("determining bucket location with getBucketLocation");
                bucketLocation = locationClient.getBucketLocation(builder -> builder.bucket(bucket)).locationConstraintAsString();
                bucketSpecificClient = this.asyncClientForRegion(bucketLocation);
            } catch (S3Exception e) {
                if(e.statusCode() == 403) {
                    logger.debug("Cannot determine location of '{}' bucket directly. Attempting to obtain bucket location with headBucket operation", bucket);
                    try {
                        final HeadBucketResponse headBucketResponse = locationClient.headBucket(builder -> builder.bucket(bucket));
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
        }

        //
        // if here, no S3 nor other client has been created yet and we do not
        // have a location; we'll let it figure out from the profile region
        //
        logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucket);

        return (bucketSpecificClient != null) ? bucketSpecificClient : this.asyncClientForRegion(endpoint, bucket, null, credentials);
    }

    // --------------------------------------------------------- private methods

    private S3Client clientForRegion(String endpoint, String bucket, String region, AwsCredentials credentials) {
        // It may be useful to further cache clients for regions although at some point clients for buckets may need to be
        // specialized beyond just region end points.
        logger.debug("bucket region is: '{}'", region);

        S3ClientBuilder clientBuilder =  S3Client.builder()
            //.region(region)
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

        if ((endpoint != null) && (endpoint.length() > 0)) {
            asyncClientBuilder.endpointOverride(URI.create(configuration.getEndpointProtocol() + "://" + endpoint));
        }

        if (credentials != null) {
            asyncClientBuilder.credentialsProvider(() -> credentials);
        }

        return clientBuilder.build();
    }

    private S3Client clientForRegion(String regionString) {
        return clientForRegion(null, null, regionString, null);
    }

    private S3AsyncClient asyncClientForRegion(String endpoint, String bucket, String region, AwsCredentials credentials){
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

        if ((endpoint != null) && (endpoint.length() > 0)) {
            // TODO: shall we have the protocol in the endpoint already?
            asyncClientBuilder.endpointOverride(URI.create(configuration.getEndpointProtocol() + "://" + endpoint));
        }

        if (credentials != null) {
            asyncClientBuilder.credentialsProvider(() -> credentials);
        }

        return asyncClientBuilder.build();
    }

    private S3AsyncClient asyncClientForRegion(String regionString) {
        return asyncClientForRegion(null, null, regionString, null);
    }

}
