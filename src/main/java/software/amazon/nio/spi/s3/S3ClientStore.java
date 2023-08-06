/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;


import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Singleton cache of clients for buckets configured for the region of those buckets
 *
 * @deprecated This class is not used any more and should not be used in new
 *             implementations as it will be removed in a later version. It has
 *             been replaced by {@link S3ClientProvider} which provides the same
 *             functionality but the singleton instance. Now many instances of
 *             a {@code S3(Async)Client} can be created, each accessing its own
 *             bucket with its own connection settings.
 */
@Deprecated
public class S3ClientStore {

    private static final S3ClientStore instance = new S3ClientStore();


    private final Map<String, S3Client> bucketToClientMap = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, S3AsyncClient> bucketToAsyncClientMap = Collections.synchronizedMap(new HashMap<>());

    Logger logger = LoggerFactory.getLogger("S3ClientStore");

    private static final S3ClientProvider DEFAULT_PROVIDER = new S3ClientProvider();
    public static final S3Client DEFAULT_CLIENT = DEFAULT_PROVIDER.universalClient(false);
    public static final S3AsyncClient DEFAULT_ASYNC_CLIENT = DEFAULT_PROVIDER.universalClient(true);

    private S3ClientStore(){}

    /**
     * Get the ClientStore instance
     *
     * @return a singleton
     *
     */
    public static S3ClientStore getInstance() { return instance; }

    protected S3ClientProvider provider = new S3ClientProvider();
    /**
     * Get an existing client or generate a new client for the named bucket if one doesn't exist
     * @param bucketName the bucket name. If this value is null or empty a default client is returned
     * @return a client
     */
    public S3Client getClientForBucketName( String bucketName ) {
        logger.debug("obtaining client for bucket '{}'", bucketName);
        if (bucketName == null || bucketName.trim().equals("")) {
            return provider.universalClient();
        }

        return bucketToClientMap.computeIfAbsent(bucketName, provider::generateClient);
    }

    /**
     * Get an existing async client or generate a new client for the named bucket if one doesn't exist
     * @param bucketName the bucket name. If this value is null or empty a default client is returned
     * @return a client
     */
    public S3AsyncClient getAsyncClientForBucketName( String bucketName ) {
        logger.debug("obtaining async client for bucket '{}'", bucketName);
        if (bucketName == null || bucketName.trim().equals("")) {
            return provider.universalClient(true);
        }

        return bucketToAsyncClientMap.computeIfAbsent(bucketName, provider::generateAsyncClient);
    }


}
