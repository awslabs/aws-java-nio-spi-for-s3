/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

@ExtendWith(MockitoExtension.class)
public class S3ClientProviderTest {

    @Mock
    S3AsyncClient mockClient; //client used to determine bucket location

    S3ClientProvider provider;

    @BeforeEach
    public void before() {
        provider = new S3ClientProvider(null);
    }

    @Test
    public void initialization() {
        final var P = new S3ClientProvider(null);

        assertNotNull(P.configuration);

        S3AsyncClient t = P.universalClient();
        assertNotNull(t);

        var config = new S3NioSpiConfiguration();
        assertSame(config, new S3ClientProvider(config).configuration);
    }

    @Test
    public void testGenerateAsyncClientWithNoErrors() throws ExecutionException, InterruptedException {
        when(mockClient.headBucket(anyConsumer()))
                .thenReturn(CompletableFuture.completedFuture(
                        HeadBucketResponse.builder().bucketRegion("us-west-2").build()));
        final var s3Client = provider.generateClient("test-bucket", mockClient);
        assertNotNull(s3Client);
    }

    @Test
    public void testGenerateAsyncClientWith403Response() throws ExecutionException, InterruptedException {
        // when you get a forbidden response from HeadBucket
        when(mockClient.headBucket(anyConsumer())).thenReturn(
                CompletableFuture.failedFuture(S3Exception.builder().statusCode(403).build())
        );

        // you should fall back to a get bucket location attempt from the universal client
        var mockUniversalClient = mock(S3AsyncClient.class);
        provider.universalClient(mockUniversalClient);
        when(mockUniversalClient.getBucketLocation(anyConsumer())).thenReturn(CompletableFuture.completedFuture(
                GetBucketLocationResponse.builder()
                        .locationConstraint("us-west-2")
                        .build()
        ));

        // which should get you a client
        final var s3Client = provider.generateClient("test-bucket", mockClient);
        assertNotNull(s3Client);

        final var inOrder = inOrder(mockClient, mockUniversalClient);
        inOrder.verify(mockClient).headBucket(anyConsumer());
        inOrder.verify(mockUniversalClient).getBucketLocation(anyConsumer());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void generateAsyncClientByEndpointBucketCredentials() {
        // GIVEN
        // use a spy to record method calls on the builder which should be invoked by the provider
        var BUILDER = spy(S3AsyncClient.crtBuilder());
        provider.asyncClientBuilder = BUILDER;
        provider.configuration.withEndpoint("endpoint1:1010");

        // WHEN
        provider.generateClient("bucket1");

        // THEN
        verify(BUILDER, times(1)).endpointOverride(URI.create("https://endpoint1:1010"));
        verify(BUILDER, times(1)).region(null);

        // GIVEN
        BUILDER = spy(S3AsyncClient.crtBuilder());
        provider.asyncClientBuilder = BUILDER;
        provider.configuration.withEndpoint("endpoint2:2020");

        // WHEN
        provider.generateClient("bucket2");

        // THEN
        verify(BUILDER, times(1)).endpointOverride(URI.create("https://endpoint2:2020"));
        verify(BUILDER, times(1)).region(null);
    }
}
