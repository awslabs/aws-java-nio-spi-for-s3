/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.BDDAssertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

class S3PutOnlyIfModifiedTest {

    @Test
    void consume_GetObjectResponse_preventPutObjectRequest_DifferentFileContents() throws IOException {
        // Given
        var option = new S3PutOnlyIfModified(new Crc64nvmeFileIntegrityCheck());
        var response = mock(GetObjectResponse.class);
        var file = Files.createTempFile(getClass().getSimpleName(), "");
        Files.write(file, new byte[] { 'a', 'b', 'c' });

        // When
        option.consume(response, file);
        Files.write(file, new byte[] { 'a', 'b', 'c', 'd' });

        // Then
        then(option.preventPutObjectRequest(file)).isFalse();
        verifyNoInteractions(response);
    }

    @Test
    void consume_GetObjectResponse_preventPutObjectRequest_SameFileContents() throws IOException {
        // Given
        var option = new S3PutOnlyIfModified(new Crc64nvmeFileIntegrityCheck());
        var getObjectResponse = mock(GetObjectResponse.class);
        var file = Files.createTempFile(getClass().getSimpleName(), "");
        Files.write(file, new byte[] { 'a', 'b', 'c' });

        // When
        option.consume(getObjectResponse, file);

        // Then
        then(option.preventPutObjectRequest(file)).isTrue();
        verifyNoInteractions(getObjectResponse);
    }

    @Test
    void consume_PutObjectResponse_preventPutObjectRequest_DifferentFileContents() throws IOException {
        // Given
        var option = new S3PutOnlyIfModified(new Crc64nvmeFileIntegrityCheck());
        var response = mock(PutObjectResponse.class);
        var file = Files.createTempFile(getClass().getSimpleName(), "");
        Files.write(file, new byte[] { 'a', 'b', 'c' });

        // When
        option.consume(response, file);
        Files.write(file, new byte[] { 'a', 'b', 'c', 'd' });

        // Then
        then(option.preventPutObjectRequest(file)).isFalse();
        verifyNoInteractions(response);
    }

    @Test
    void consume_PutObjectResponse_preventPutObjectRequest_SameFileContents() throws IOException {
        // Given
        var option = new S3PutOnlyIfModified(new Crc64nvmeFileIntegrityCheck());
        var getObjectResponse = mock(PutObjectResponse.class);
        var file = Files.createTempFile(getClass().getSimpleName(), "");
        Files.write(file, new byte[] { 'a', 'b', 'c' });

        // When
        option.consume(getObjectResponse, file);

        // Then
        then(option.preventPutObjectRequest(file)).isTrue();
        verifyNoInteractions(getObjectResponse);
    }
}
