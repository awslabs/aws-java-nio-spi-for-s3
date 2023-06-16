/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
public class S3ClientStoreTest {
    S3ClientStore instance;

    @Mock
    S3Client mockClient; //client used to determine bucket location

    @Spy
    final S3ClientStore spyInstance = S3FileSystemProvider.getClientStore();  // TODO: move to S3FileSystemProvider

    @BeforeEach
    public void setUp() throws Exception {
        instance = new S3ClientStore();
    }

    @Test
    public void testGetClientForNullBucketName() {
        assertEquals(S3ClientStore.DEFAULT_CLIENT, instance.getClientForBucketName(null));
    }

    @Test
    public void testGetAsyncClientForNullBucketName() {
        assertEquals(S3ClientStore.DEFAULT_ASYNC_CLIENT, instance.getAsyncClientForBucketName(null));
    }

    @Test
    public void testGetClientForEmptyBucketName() {
        assertEquals(S3ClientStore.DEFAULT_CLIENT, instance.getClientForBucketName(""));
        assertEquals(S3ClientStore.DEFAULT_CLIENT, instance.getClientForBucketName(" "));
    }

    @Test
    public void testGetAsyncClientForEmptyBucketName() {
        assertEquals(S3ClientStore.DEFAULT_ASYNC_CLIENT, instance.getAsyncClientForBucketName(""));
        assertEquals(S3ClientStore.DEFAULT_ASYNC_CLIENT, instance.getAsyncClientForBucketName(" "));
    }

    @Test
    public void testGenerateClientWithNoErrors() {
        when(mockClient.getBucketLocation(any(Consumer.class)))
                .thenReturn(GetBucketLocationResponse.builder().locationConstraint("us-west-2").build());
        final S3Client s3Client = instance.generateClient("test-bucket", mockClient);
        assertNotNull(s3Client);

    }

    @Test
    public void testGenerateAsyncClientWithNoErrors() {
        when(mockClient.getBucketLocation(any(Consumer.class)))
                .thenReturn(GetBucketLocationResponse.builder().locationConstraint("us-west-2").build());
        final S3AsyncClient s3Client = instance.generateAsyncClient("test-bucket", mockClient);
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
        final S3Client s3Client = instance.generateClient("test-bucket", mockClient);
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
        final S3AsyncClient s3Client = instance.generateAsyncClient("test-bucket", mockClient);
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
        final S3AsyncClient s3Client = instance.generateAsyncClient("test-bucket", mockClient);
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
            instance.generateClient("test-bucket", mockClient);
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
            instance.generateAsyncClient("test-bucket", mockClient);
        } catch (Exception e) {
            assertEquals(NoSuchElementException.class, e.getClass());
        }

        final InOrder inOrder = inOrder(mockClient);
        inOrder.verify(mockClient).getBucketLocation(any(Consumer.class));
        inOrder.verify(mockClient).headBucket(any(Consumer.class));
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    // TODO: move to S3FileSystemProvider
    public void testCaching() {
        S3Client client = S3Client.builder().region(Region.US_EAST_1).build();
        doReturn(client).when(spyInstance).generateClient("test-bucket");

        final S3Client client1 = spyInstance.getClientForBucketName("test-bucket");
        verify(spyInstance).generateClient("test-bucket");
        assertSame(client1, client);

        S3Client differentClient = S3Client.builder().region(Region.US_EAST_1).build();
        assertNotSame(client, differentClient);

        lenient().doReturn(differentClient).when(spyInstance).generateClient("test-bucket");
        final S3Client client2 = spyInstance.getClientForBucketName("test-bucket");
        // same instance because second is cached.
        assertSame(client1, client2);
        assertSame(client2, client);
        assertNotSame(client2, differentClient);
    }

    @Test
    public void testAsyncCaching() {
        S3AsyncClient client = S3AsyncClient.builder().region(Region.US_EAST_1).build();
        doReturn(client).when(spyInstance).generateAsyncClient("test-bucket");

        final S3AsyncClient client1 = spyInstance.getAsyncClientForBucketName("test-bucket");
        verify(spyInstance).generateAsyncClient("test-bucket");
        assertSame(client1, client);

        S3AsyncClient differentClient = S3AsyncClient.builder().region(Region.US_EAST_1).build();
        assertNotSame(client, differentClient);

        lenient().doReturn(differentClient).when(spyInstance).generateAsyncClient("test-bucket");
        final S3AsyncClient client2 = spyInstance.getAsyncClientForBucketName("test-bucket");
        // same instance because second is cached.
        assertSame(client1, client2);
        assertSame(client2, client);
        assertNotSame(client2, differentClient);
    }
}

