/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static software.amazon.nio.spi.s3.util.TimeOutUtils.TIMEOUT_TIME_LENGTH_1;
import static software.amazon.nio.spi.s3.util.TimeOutUtils.TIMEOUT_TIME_LENGTH_3;
import static software.amazon.nio.spi.s3.util.TimeOutUtils.logAndGenerateExceptionOnTimeOut;

import java.net.URI;
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
    private static final S3AsyncClient UNIVERSAL_CLIENT = S3AsyncClient.builder()
        .endpointOverride(URI.create("https://s3.us-east-1.amazonaws.com"))
        .crossRegionAccessEnabled(true)
        .region(Region.US_EAST_1)
        .build();

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

    public S3ClientProvider(S3NioSpiConfiguration c) {
        this.configuration = (c == null) ? new S3NioSpiConfiguration() : c;
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
        return UNIVERSAL_CLIENT;
    }

    /**
     * Generates a sync client for the named bucket using the provided location
     * discovery client.
     *
     * @param bucket the named of the bucket to make the client for
     * @return an S3 client appropriate for the region of the named bucket
     */
    protected S3AsyncClient generateClient(String bucket) {
        try {
            return generateClient(bucket, universalClient());
        } catch (ExecutionException | InterruptedException e) {
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

        return configureCrtClientForRegion(Optional.ofNullable(bucketLocation).orElse(configuration.getRegion()));
    }

//    private String determineBucketLocation(String bucketName, S3AsyncClient locationClient)
//            throws ExecutionException, InterruptedException {
//
//        return getBucketLocation(bucketName, locationClient);
////        } catch (ExecutionException  e) {
////            if (e.getCause() instanceof S3Exception && isForbidden((S3Exception) e.getCause())) {
////
////                logger.debug("Cannot determine location of '{}' bucket directly", bucketName);
////                return getBucketLocationFromHead(bucketName, locationClient);
////            } else {
////                throw e;
////            }
//
//    }

    private String getBucketLocation(String bucketName, S3AsyncClient locationClient)
            throws ExecutionException, InterruptedException {
        logger.debug("checking if the bucket is in the same region as the current profile using HeadBucket");
        try (var client = S3AsyncClient.create()) {
            final HeadBucketResponse response = client
                    .headBucket(builder -> builder.bucket(bucketName))
                    .get(TIMEOUT_TIME_LENGTH_3, SECONDS);
            return response.bucketRegion();

        } catch (TimeoutException e) {
            throw logAndGenerateExceptionOnTimeOut(
                    logger,
                    "generateClient",
                    TIMEOUT_TIME_LENGTH_1,
                    MINUTES);
        } catch (Throwable t) {

            if (t instanceof ExecutionException &&
                    t.getCause() instanceof S3Exception &&
                    ((S3Exception) t.getCause()).statusCode() == 301) {
                // redirect region should be in the header
                logger.debug("HeadBucket was unsuccessful, redirect received, attempting to extract x-amz-bucket-region header");
                S3Exception s3e = (S3Exception) t.getCause();
                final var matchingHeaders = s3e.awsErrorDetails().sdkHttpResponse().matchingHeaders("x-amz-bucket-region");
                if (matchingHeaders != null && !matchingHeaders.isEmpty()) {
                    return matchingHeaders.get(0);
                }
            }

            logger.debug("HeadBucket failed and no redirect. Attempting a call to GetBucketLocation");
            try {
                return locationClient.getBucketLocation(builder -> builder.bucket(bucketName))
                        .get(TIMEOUT_TIME_LENGTH_1, MINUTES).locationConstraintAsString();
            } catch (TimeoutException e) {
                throw logAndGenerateExceptionOnTimeOut(
                        logger,
                        "generateClient",
                        TIMEOUT_TIME_LENGTH_1,
                        MINUTES);
            }
        }
    }

//    private String getBucketLocationFromHead(String bucketName, S3AsyncClient locationClient)
//            throws ExecutionException, InterruptedException {
//        try {
//            logger.debug("Attempting to obtain bucket '{}' location with headBucket operation", bucketName);
//            final var headBucketResponse = locationClient.headBucket(builder -> builder.bucket(bucketName));
//            return getBucketRegionFromResponse(headBucketResponse.get(TIMEOUT_TIME_LENGTH_1, MINUTES).sdkHttpResponse());
//        } catch (ExecutionException e) {
//            if (e.getCause() instanceof S3Exception && isRedirect((S3Exception) e.getCause())) {
//                var s3e = (S3Exception) e.getCause();
//                return getBucketRegionFromResponse(s3e.awsErrorDetails().sdkHttpResponse());
//            } else {
//                throw e;
//            }
//        } catch (TimeoutException e) {
//            throw logAndGenerateExceptionOnTimeOut(
//                    logger,
//                    "generateClient",
//                    TIMEOUT_TIME_LENGTH_1,
//                    MINUTES);
//        }
//    }

    private boolean isForbidden(S3Exception e) {
        return e.statusCode() == 403;
    }
//
//    private boolean isRedirect(S3Exception e) {
//        return e.statusCode() == 301;
//    }

//    private String getBucketRegionFromResponse(SdkHttpResponse response) {
//        return response.firstMatchingHeader("x-amz-bucket-region").orElseThrow(() ->
//            new NoSuchElementException("Head Bucket Response doesn't include the header 'x-amz-bucket-region'")
//        );
//    }

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
