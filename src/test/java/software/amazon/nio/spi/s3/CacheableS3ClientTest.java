/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@ExtendWith(MockitoExtension.class)
class CacheableS3ClientTest {

    CacheableS3Client cacheableS3Client;
    @Mock
    S3AsyncClient wrappedClient;

    @BeforeEach
    void setUp() {
        cacheableS3Client = new CacheableS3Client(wrappedClient);
    }



    @Test
    void closeSetsIsClosedToTrue() {
        cacheableS3Client.close();
        assertTrue(cacheableS3Client.isClosed());
    }

    @Test
    void closeCallsCloseOnWrappedClient() {
        cacheableS3Client.close();
        Mockito.verify(wrappedClient, Mockito.times(1)).close();
    }

    @Test
    void isClosedIsFalseWhenCreated() {
        assertFalse(cacheableS3Client.isClosed());
    }
}