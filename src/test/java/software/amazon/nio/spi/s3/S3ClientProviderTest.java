/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.InOrder;
import org.mockito.Mock;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
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
    public void before() throws Exception {
        provider = new S3ClientProvider();
    }

    @Test
    public void initialization() {
        final S3ClientProvider P = new S3ClientProvider();

        assertNotNull(P.configuration);

        assertTrue(P.universalClient() instanceof S3Client);
        assertNotNull(P.universalClient());

        assertTrue(P.universalClient(true) instanceof S3AsyncClient);
        assertNotNull(P.universalClient());

        S3NioSpiConfiguration config = new S3NioSpiConfiguration();
        assertSame(config, new S3ClientProvider(config).configuration);
    }

    @Test
    public void testGenerateAsyncClientWithNoErrors() {
        when(mockClient.getBucketLocation(any(Consumer.class)))
                .thenReturn(GetBucketLocationResponse.builder().locationConstraint("us-west-2").build());
        final S3AsyncClient s3Client = provider.generateAsyncClient("test-bucket", mockClient);
        assertNotNull(s3Client);
    }

    @Test
    public void testGenerateClientWith403Response() {
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(any(Consumer.class))).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // you should fall back to a head bucket attempt
        when(mockClient.headBucket(any(Consumer.class)))
                .thenReturn((HeadBucketResponse) HeadBucketResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .putHeader("x-amz-bucket-region", "us-west-2")
                                .build())
                        .build());

        // which should get you a client
        final S3Client s3Client = provider.generateClient("test-bucket", mockClient);
        assertNotNull(s3Client);

        final InOrder inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(any(Consumer.class));
        inOrder.verify(mockClient).headBucket(any(Consumer.class));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGenerateAsyncClientWith403Response() {
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(any(Consumer.class))).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // you should fall back to a head bucket attempt
        when(mockClient.headBucket(any(Consumer.class)))
                .thenReturn((HeadBucketResponse) HeadBucketResponse.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .putHeader("x-amz-bucket-region", "us-west-2")
                                .build())
                        .build());

        // which should get you a client
        final S3AsyncClient s3Client = provider.generateAsyncClient("test-bucket", mockClient);
        assertNotNull(s3Client);

        final InOrder inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(any(Consumer.class));
        inOrder.verify(mockClient).headBucket(any(Consumer.class));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGenerateAsyncClientWith403Then301Responses(){
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(any(Consumer.class))).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // and you get a 301 response on headBucket
        when(mockClient.headBucket(any(Consumer.class))).thenThrow(
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
        final S3AsyncClient s3Client = provider.generateAsyncClient("test-bucket", mockClient);
        assertNotNull(s3Client);

        final InOrder inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(any(Consumer.class));
        inOrder.verify(mockClient).headBucket(any(Consumer.class));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testGenerateClientWith403Then301ResponsesNoHeader(){
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(any(Consumer.class))).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // and you get a 301 response on headBucket but no header for region
        when(mockClient.headBucket(any(Consumer.class))).thenThrow(
                S3Exception.builder()
                        .statusCode(301)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .sdkHttpResponse(SdkHttpResponse.builder()
                                        .build())
                                .build())
                        .build()
        );

        // then you should get a NoSuchElement exception when you try to get the header
        try {
            provider.generateClient("test-bucket", mockClient);
        } catch (Exception e) {
            assertEquals(NoSuchElementException.class, e.getClass());
        }

        final InOrder inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(any(Consumer.class));
        inOrder.verify(mockClient).headBucket(any(Consumer.class));
        inOrder.verifyNoMoreInteractions();
    }


    @Test
    public void testGenerateAsyncClientWith403Then301ResponsesNoHeader(){
        // when you get a forbidden response from getBucketLocation
        when(mockClient.getBucketLocation(any(Consumer.class))).thenThrow(
                S3Exception.builder().statusCode(403).build()
        );
        // and you get a 301 response on headBucket but no header for region
        when(mockClient.headBucket(any(Consumer.class))).thenThrow(
                S3Exception.builder()
                        .statusCode(301)
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .sdkHttpResponse(SdkHttpResponse.builder()
                                        .build())
                                .build())
                        .build()
        );

        // then you should get a NoSuchElement exception when you try to get the header
        try {
            provider.generateAsyncClient("test-bucket", mockClient);
        } catch (Exception e) {
            assertEquals(NoSuchElementException.class, e.getClass());
        }

        final InOrder inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(any(Consumer.class));
        inOrder.verify(mockClient).headBucket(any(Consumer.class));
        inOrder.verifyNoMoreInteractions();
    }
}
