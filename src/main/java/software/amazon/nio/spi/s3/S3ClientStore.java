/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.*;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

/**
 * A Singleton cache of clients for buckets configured for the region of those buckets
 */
public class S3ClientStore {

    private static final S3ClientStore instance = new S3ClientStore();

    /**
     * Default S3CrtAsyncClientBuilder
     */
    protected S3CrtAsyncClientBuilder asyncClientBuilder = S3AsyncClient.crtBuilder();

    /**
     * Default client using the "https://s3.us-east-1.amazonaws.com" endpoint
     */
    public static final S3Client DEFAULT_CLIENT = S3Client.builder()
            .endpointOverride(URI.create("https://s3.us-east-1.amazonaws.com"))
            .region(Region.US_EAST_1)
            .build();

    /**
     * Default asynchronous client using the "https://s3.us-east-1.amazonaws.com" endpoint
     */
    public static final S3AsyncClient DEFAULT_ASYNC_CLIENT = S3AsyncClient.builder()
            .endpointOverride(URI.create("https://s3.us-east-1.amazonaws.com"))
            .region(Region.US_EAST_1)
            .build();

    private final Map<String, S3Client> bucketToClientMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, S3AsyncClient> bucketToAsyncClientMap = Collections.synchronizedMap(new HashMap<>());

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

    Logger logger = LoggerFactory.getLogger("S3ClientStore");



    /**
     * Get the ClientStore instance
     *
     * @deprecated
     * The use of S3ClientStore as singleton is deprecated in favour of creating
     * normal instances optionally with additional configuration settings.
     *
     * @return a singleton
     */
    @Deprecated
    public static S3ClientStore getInstance() { return instance; }
    protected S3ClientStore(){}

    /**
     * Get an existing client or generate a new client for the named bucket if one doesn't exist
     * @param bucketName the bucket name. If this value is null or empty a default client is returned
     * @return a client
     */
    public S3Client getClientForBucketName( String bucketName ) {
        logger.debug("obtaining client for bucket '{}'", bucketName);
        if (bucketName == null || bucketName.trim().equals("")) {
            return DEFAULT_CLIENT;
        }

        return bucketToClientMap.computeIfAbsent(bucketName, this::generateClient);
    }

    /**
     * Get an existing async client or generate a new client for the named bucket if one doesn't exist
     * @param bucketName the bucket name. If this value is null or empty a default client is returned
     * @return a client
     */
    public S3AsyncClient getAsyncClientForBucketName( String bucketName ) {
        logger.debug("obtaining async client for bucket '{}'", bucketName);
        if (bucketName == null || bucketName.trim().equals("")) {
            return DEFAULT_ASYNC_CLIENT;
        }

        return bucketToAsyncClientMap.computeIfAbsent(bucketName, this::generateAsyncClient);
    }

    /**
     * Generate a client for the named bucket using a default client to determine the location of the named bucket
     * @param bucketName the named of the bucket to make the client for
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3Client generateClient(String bucketName){
        return this.generateClient(bucketName, DEFAULT_CLIENT);
    }

    /**
     * Generate an asynchronous client for the named bucket using a default client to determine the location of the named bucket
     * @param bucketName the named of the bucket to make the client for
     * @return an asynchronous S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateAsyncClient(String bucketName){
        return this.generateAsyncClient(bucketName, DEFAULT_CLIENT);
    }

    /**
     * Generate a client for the named bucket using a default client to determine the location of the named client
     * @param bucketName the named of the bucket to make the client for
     * @param locationClient the client used to determine the location of the named bucket, recommend using DEFAULT_CLIENT
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3Client generateClient (String bucketName, S3Client locationClient) {
        logger.debug("generating client for bucket: '{}'", bucketName);

        S3Client bucketSpecificClient = null;

        String[] elements = bucketName.split(S3Path.PATH_SEPARATOR);
        //
        // if elements has only 1 element, no endpoint is given and an s3 endpoint
        // is assumend. In this case, the location of the bucket is determined
        // using the locationClient
        //
        if (elements.length == 1) {
            String bucketLocation = null;
            try {
                logger.debug("determining bucket location with getBucketLocation");
                bucketLocation = locationClient.getBucketLocation(builder -> builder.bucket(bucketName)).locationConstraintAsString();
                bucketSpecificClient = this.clientForRegion(bucketName, bucketLocation);
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
        }

        //
        // if here, no S3 not other client has been created yet and we do not
        // have a location; we'll let it figure out from the profile region
        //
        logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucketName);

        return (bucketSpecificClient != null) ? bucketSpecificClient : this.clientForRegion(elements[0], null);
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

        String[] elements = bucketName.split(S3Path.PATH_SEPARATOR);
        //
        // if elements has only 1 element, no endpoint is given and an s3 endpoint
        // is assumend. In this case, the location of the bucket is determined
        // using the locationClient
        //
        if (elements.length == 1) {
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
        // if here, no S3 not other client has been created yet and we do not
        // have a location; we'll let it figure out from the profile region
        //
        logger.warn("Unable to determine the region of bucket: '{}'. Generating a client for the profile region.", bucketName);

        return (bucketSpecificClient != null) ? bucketSpecificClient : this.asyncClientForRegion(elements[0], null);
    }

    private S3Client clientForRegion(String endpoint, String regionString) {
        // It may be useful to further cache clients for regions although at some point clients for buckets may need to be
        // specialized beyond just region end points.
        logger.debug("bucket region is: '{}'", regionString);

        S3ClientBuilder clientBuilder =  S3Client.builder()
            //.region(region)
            .overrideConfiguration(conf -> conf.retryPolicy(builder -> builder
                    .retryCondition(retryCondition)
                    .backoffStrategy(backoffStrategy)));

        //
        // If no regionString is provided, the builder will try with the
        // profile's region setting
        //
        if ((regionString != null) && (!regionString.trim().equals(""))) {
            clientBuilder.region(Region.of(regionString));
        }

        if ((endpoint != null) && (endpoint.length() > 0)) {
            //
            // endpoint may contain credentials in the form of key:secret@url
            //
            String key = null, secret = null;
            int pos = endpoint.indexOf('@');
            if (pos >= 1) {
                key = endpoint.substring(0, pos);
                endpoint = endpoint.substring(pos+1);
                pos = key.indexOf(':');
                if (pos >= 0) {
                    secret = key.substring(pos+1);
                    key = key.substring(0, pos);
                }
            }
            // TODO: shall we have the protocol in the endpoint already?
            clientBuilder.endpointOverride(URI.create("https://" + endpoint));

            if (key != null) {
                final AwsCredentials credentials = AwsBasicCredentials.create(key, secret);
                clientBuilder.credentialsProvider(() -> credentials);
            }
        }

        return clientBuilder.build();
    }

    private S3Client clientForRegion(String regionString) {
        return clientForRegion(null, regionString);
    }

    private S3AsyncClient asyncClientForRegion(String endpoint, String regionString){
        // It may be useful to further cache clients for regions although at some point clients for buckets may need to be
        // specialized beyond just region end points.
        logger.debug("bucket region is: '{}'", regionString);

        System.out.println(asyncClientBuilder.getClass());

        //
        // If no regionString is provided, the builder will try with the
        // profile's region setting
        //
        if ((regionString != null) && (!regionString.trim().equals(""))) {
            asyncClientBuilder.region(Region.of(regionString));
        }

        if ((endpoint != null) && (endpoint.length() > 0)) {
            //
            // endpoint may contain credentials in the form of key:secret@url
            //
            String key = null, secret = null;
            int pos = endpoint.indexOf('@');
            if (pos >= 1) {
                key = endpoint.substring(0, pos);
                endpoint = endpoint.substring(pos+1);
                pos = key.indexOf(':');
                if (pos >= 0) {
                    secret = key.substring(pos+1);
                    key = key.substring(0, pos);
                }
            }
            // TODO: shall we have the protocol in the endpoint already?
            asyncClientBuilder.endpointOverride(URI.create("https://" + endpoint));

            if (key != null) {
                final AwsCredentials credentials = AwsBasicCredentials.create(key, secret);
                asyncClientBuilder.credentialsProvider(() -> credentials);
            }
        }

        return asyncClientBuilder.build();
    }

    private S3AsyncClient asyncClientForRegion(String regionString) {
        return asyncClientForRegion(null, regionString);
    }
}
