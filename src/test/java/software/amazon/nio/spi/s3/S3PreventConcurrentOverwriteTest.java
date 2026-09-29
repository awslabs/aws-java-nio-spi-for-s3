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
import java.nio.file.Path;

class S3PreventConcurrentOverwriteTest {

    @Test
    void apply_PutObjectRequest_SetsIfMatchHeader() {
        // Given
        S3PreventConcurrentOverwrite option = new S3PreventConcurrentOverwrite();
        GetObjectResponse getResponse = GetObjectResponse.builder().eTag("test-etag").build();
        option.consume(getResponse, null);

        PutObjectRequest.Builder putBuilder = mock(PutObjectRequest.Builder.class);
        when(putBuilder.ifMatch(anyString())).thenReturn(putBuilder);

        // When
        option.apply(putBuilder, null);

        // Then
        verify(putBuilder).ifMatch("test-etag");
    }

    @Test
    void consume_GetObjectResponse_StoresETag() {
        // Given
        S3PreventConcurrentOverwrite option = new S3PreventConcurrentOverwrite();
        GetObjectResponse getResponse = GetObjectResponse.builder().eTag("test-etag").build();
        
        // When
        option.consume(getResponse, null);
        
        // Then
        PutObjectRequest.Builder putBuilder = mock(PutObjectRequest.Builder.class);
        when(putBuilder.ifMatch(anyString())).thenReturn(putBuilder);
        option.apply(putBuilder, null);
        verify(putBuilder).ifMatch("test-etag");
    }

    @Test
    void consume_PutObjectResponse_StoresETag() {
        // Given
        S3PreventConcurrentOverwrite option = new S3PreventConcurrentOverwrite();
        PutObjectResponse putResponse = PutObjectResponse.builder().eTag("new-etag").build();
        Path file = mock(Path.class);

        // When
        option.consume(putResponse, file);

        // Then
        PutObjectRequest.Builder putBuilder = mock(PutObjectRequest.Builder.class);
        when(putBuilder.ifMatch(anyString())).thenReturn(putBuilder);
        option.apply(putBuilder, null);
        verify(putBuilder).ifMatch("new-etag");
        verifyNoInteractions(file);
    }

    @Test
    void consume_UpdatesETagWhenCalledMultipleTimes() {
        // Given
        S3PreventConcurrentOverwrite option = new S3PreventConcurrentOverwrite();
        GetObjectResponse getResponse = GetObjectResponse.builder().eTag("first-etag").build();
        PutObjectResponse putResponse = PutObjectResponse.builder().eTag("second-etag").build();
        Path file = mock(Path.class);

        // When
        option.consume(getResponse, file);
        option.consume(putResponse, file);
        
        // Then
        PutObjectRequest.Builder putBuilder = mock(PutObjectRequest.Builder.class);
        when(putBuilder.ifMatch(anyString())).thenReturn(putBuilder);
        option.apply(putBuilder, null);
        verify(putBuilder).ifMatch("second-etag");
        verifyNoInteractions(file);
    }
}
