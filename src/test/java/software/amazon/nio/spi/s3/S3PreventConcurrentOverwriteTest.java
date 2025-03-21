/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3PreventConcurrentOverwriteTest {

    @Test
    void test() {
        var eTag = "some-etag";
        var preventConcurrentOverwrite = new S3PreventConcurrentOverwrite();

        var getObjectResponse = mock(GetObjectResponse.class);
        when(getObjectResponse.eTag()).thenReturn(eTag);
        preventConcurrentOverwrite.consume(getObjectResponse);
        verify(getObjectResponse, times(1)).eTag();

        var putObjectRequest = mock(PutObjectRequest.Builder.class);
        preventConcurrentOverwrite.apply(putObjectRequest);
        verify(putObjectRequest, times(1)).ifMatch(eTag);
    }

}
