/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

@ExtendWith(MockitoExtension.class)
public class S3ClientProviderTest {

    @Mock
    S3Client mockClient; //client used to determine bucket location

    S3ClientProvider provider;

    @BeforeEach
    public void before() {
        provider = new S3ClientProvider(null);
    }

    @Test
    public void initialization() {
        final var P = new S3ClientProvider(null);

        assertNotNull(P.configuration);

        assertNotNull(P.universalClient());
        assertThat(P.universalClient()).isInstanceOf(S3Client.class);

        S3AsyncClient t = P.universalClient(true);
        assertNotNull(t);
        assertThat(t).isInstanceOf(S3AsyncClient.class);

        var config = new S3NioSpiConfiguration();
        assertSame(config, new S3ClientProvider(config).configuration);
    }

    @Test
    public void testGenerateAsyncClientWithNoErrors() {
        when(mockClient.getBucketLocation(anyConsumer()))
                .thenReturn(GetBucketLocationResponse.builder().locationConstraint("us-west-2").build());
        final var s3Client = provider.generateAsyncClient("test-bucket", mockClient, true);
        assertNotNull(s3Client);
    }

    @Test
    public void testGenerateClientWith403Response() {
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(anyConsumer())).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // you should fall back to a head bucket attempt
        when(mockClient.headBucket(anyConsumer()))
                .thenReturn((HeadBucketResponse) HeadBucketResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .putHeader("x-amz-bucket-region", "us-west-2")
                                .build())
                        .build());

        // which should get you a client
        final var s3Client = provider.generateSyncClient("test-bucket", mockClient);
        assertNotNull(s3Client);

        final var inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(anyConsumer());
        inOrder.verify(mockClient).headBucket(anyConsumer());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGenerateAsyncClientWith403Response() {
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(anyConsumer())).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // you should fall back to a head bucket attempt
        when(mockClient.headBucket(anyConsumer()))
                .thenReturn((HeadBucketResponse) HeadBucketResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .putHeader("x-amz-bucket-region", "us-west-2")
                                .build())
                        .build());

        // which should get you a client
        final var s3Client = provider.generateAsyncClient("test-bucket", mockClient, true);
        assertNotNull(s3Client);

        final var inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(anyConsumer());
        inOrder.verify(mockClient).headBucket(anyConsumer());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGenerateAsyncClientWith403Then301Responses(){
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(anyConsumer())).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // and you get a 301 response on headBucket
        when(mockClient.headBucket(anyConsumer())).thenThrow(
                S3Exception.builder()
                        .statusCode(301)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .sdkHttpResponse(SdkHttpResponse.builder()
                                        .putHeader("x-amz-bucket-region", "us-west-2")
                                        .build())
                                .build())
                        .build()
        );

        // then you should be able to get a client as long as the error response header contains the region
        final var s3Client = provider.generateAsyncClient("test-bucket", mockClient, true);
        assertNotNull(s3Client);

        final var inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(anyConsumer());
        inOrder.verify(mockClient).headBucket(anyConsumer());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGenerateClientWith403Then301ResponsesNoHeader(){
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(anyConsumer())).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // and you get a 301 response on headBucket but no header for region
        when(mockClient.headBucket(anyConsumer())).thenThrow(
                S3Exception.builder()
                        .statusCode(301)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .sdkHttpResponse(SdkHttpResponse.builder()
                                        .build())
                                .build())
                        .build()
        );

        // then you should get a NoSuchElement exception when you try to get the header
        assertThrows(NoSuchElementException.class, () -> provider.generateSyncClient("test-bucket", mockClient));

        final var inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(anyConsumer());
        inOrder.verify(mockClient).headBucket(anyConsumer());
        inOrder.verifyNoMoreInteractions();
    }


    @Test
    public void testGenerateAsyncClientWith403Then301ResponsesNoHeader(){
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(anyConsumer())).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // and you get a 301 response on headBucket but no header for region
        when(mockClient.headBucket(anyConsumer())).thenThrow(
                S3Exception.builder()
                        .statusCode(301)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .sdkHttpResponse(SdkHttpResponse.builder()
                                        .build())
                                .build())
                        .build()
        );

        // then you should get a NoSuchElement exception when you try to get the header
        assertThrows(NoSuchElementException.class, () -> provider.generateAsyncClient("test-bucket", mockClient, true));

        final var inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(anyConsumer());
        inOrder.verify(mockClient).headBucket(anyConsumer());
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void generateAsyncClientByEndpointBucketCredentials() {
        final var BUILDER = new FakeAsyncS3ClientBuilder();
        provider.asyncClientBuilder = BUILDER;

        provider.configuration.withEndpoint("endpoint1:1010");
        provider.generateAsyncClient("bucket1", true);
        then(BUILDER.endpointOverride.toString()).isEqualTo("https://endpoint1:1010");
        then(BUILDER.region).isEqualTo(Region.US_EAST_1);  // just a default in the case not provide

        provider.configuration.withEndpoint("endpoint2:2020");
        provider.generateAsyncClient("bucket2", true);
        then(BUILDER.endpointOverride.toString()).isEqualTo("https://endpoint2:2020");
        then(BUILDER.region).isEqualTo(Region.US_EAST_1);  // just a default in the case not provide
    }
}
