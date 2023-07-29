/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
@Deprecated
public class S3ClientStoreTest {
    S3ClientStore instance;

    @Mock
    S3Client mockClient; //client used to determine bucket location


    @BeforeEach
    public void setUp() throws Exception {
        instance = S3ClientStore.getInstance();
    }

    @Test
    public void testGetClientForNullBucketName() {
        assertEquals(instance.provider.universalClient(), instance.getClientForBucketName(null));
    }

    @Test
    public void testGetAsyncClientForNullBucketName() {
        assertEquals(instance.provider.universalClient(true), instance.getAsyncClientForBucketName(null));
    }

    @Test
    public void testGetClientForEmptyBucketName() {
        assertEquals(instance.provider.universalClient(), instance.getClientForBucketName(""));
        assertEquals(instance.provider.universalClient(), instance.getClientForBucketName(" "));
    }

    @Test
    public void testGetAsyncClientForEmptyBucketName() {
        assertEquals(instance.provider.universalClient(true), instance.getAsyncClientForBucketName(""));
        assertEquals(instance.provider.universalClient(true), instance.getAsyncClientForBucketName(" "));
    }

    @Test
    public void testCaching() throws Exception {
        instance.provider = new S3ClientProvider() {
            @Override
            protected S3Client generateClient(String bucketName) {
                return S3Client.create();
            }
        };

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-east-1");
            final S3Client client1 = instance.getClientForBucketName("test-bucket1");
            assertSame(client1, instance.getClientForBucketName("test-bucket1"));
            assertNotSame(client1, instance.getClientForBucketName("test-bucket2"));
        });
    }

    @Test
    public void testAsyncCaching() throws Exception {
        instance.provider = new S3ClientProvider() {
            @Override
            protected S3AsyncClient generateAsyncClient(String bucketName) {
                return S3AsyncClient.create();
            }
        };

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-east-1");
            final S3AsyncClient client1 = instance.getAsyncClientForBucketName("test-bucket1");
            assertSame(client1, instance.getAsyncClientForBucketName("test-bucket1"));
            assertNotSame(client1, instance.getAsyncClientForBucketName("test-bucket2"));
        });
    }

}

