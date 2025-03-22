/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

class S3PreventConcurrentOverwriteTest {

    @Test
    void test_consume_GetObjectResponse() {
        var preventConcurrentOverwrite = new S3PreventConcurrentOverwrite();

        var eTag = "some.etag";
        var getObjectResponse = mock(GetObjectResponse.class);
        when(getObjectResponse.eTag()).thenReturn(eTag);
        preventConcurrentOverwrite.consume(getObjectResponse);
        verify(getObjectResponse, times(1)).eTag();

        var putObjectRequest = mock(PutObjectRequest.Builder.class);
        preventConcurrentOverwrite.apply(putObjectRequest);
        verify(putObjectRequest, times(1)).ifMatch(eTag);

    }

    @Test
    void test_consume_PutObjectResponse() {
        var preventConcurrentOverwrite = new S3PreventConcurrentOverwrite();

        var eTag = "some-etag";
        var putObjectResponse = mock(PutObjectResponse.class);
        when(putObjectResponse.eTag()).thenReturn(eTag);
        preventConcurrentOverwrite.consume(putObjectResponse);
        verify(putObjectResponse, times(1)).eTag();

        var putObjectRequest = mock(PutObjectRequest.Builder.class);
        preventConcurrentOverwrite.apply(putObjectRequest);
        verify(putObjectRequest, times(1)).ifMatch(eTag);

    }

}
