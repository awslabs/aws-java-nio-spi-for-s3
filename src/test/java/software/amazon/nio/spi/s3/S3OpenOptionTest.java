/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssertions.*;
import static org.mockito.Mockito.*;

class S3OpenOptionTest {

    @Test
    void preventConcurrentOverwrite_ReturnsS3PreventConcurrentOverwriteInstance() {
        // When
        S3OpenOption option = S3OpenOption.preventConcurrentOverwrite();

        // Then
        assertThat(option)
                .isInstanceOf(S3PreventConcurrentOverwrite.class)
                .isInstanceOf(OpenOption.class);
    }

    @Test
    void putOnlyIfModified_ReturnsS3PutOnlyIfModified() {
        // When
        S3OpenOption option = S3OpenOption.putOnlyIfModified();

        // Then
        assertThat(option)
            .isInstanceOf(S3PutOnlyIfModified.class)
            .isInstanceOf(OpenOption.class);
    }

    @Test
    void putOnlyIfModified_ReturnsS3PutOnlyIfModified_withChecksumAlgorithm() {
        // When
        S3OpenOption option = S3OpenOption.putOnlyIfModified(new Crc32FileIntegrityCheck());

        // Then
        assertThat(option)
            .isInstanceOf(S3PutOnlyIfModified.class)
            .isInstanceOf(OpenOption.class);
    }

    @Test
    void range_WithEndOnly_ReturnsS3RangeHeaderInstance() {
        // When
        S3OpenOption option = S3OpenOption.range(100);

        // Then
        assertThat(option)
                .isInstanceOf(S3RangeHeader.class)
                .isInstanceOf(OpenOption.class);

        // Verify the range is set correctly
        GetObjectRequest.Builder builder = GetObjectRequest.builder();
        option.apply(builder);
        assertThat(builder.build().range()).isEqualTo("bytes=0-100");
    }

    @Test
    void range_WithStartAndEnd_ReturnsS3RangeHeaderInstance() {
        // When
        S3OpenOption option = S3OpenOption.range(50, 100);

        // Then
        assertThat(option)
                .isInstanceOf(S3RangeHeader.class)
                .isInstanceOf(OpenOption.class);

        // Verify the range is set correctly
        GetObjectRequest.Builder builder = GetObjectRequest.builder();
        option.apply(builder);
        assertThat(builder.build().range()).isEqualTo("bytes=50-100");
    }

    @Test
    void retainAll() {
        // When
        var option1 = StandardOpenOption.CREATE;
        var option2 = S3OpenOption.preventConcurrentOverwrite();
        var option3 = StandardOpenOption.WRITE;
        var option4 = S3OpenOption.putOnlyIfModified();
        var options = Set.of(option1, option2, option3, option4);

        // Then
        then(S3OpenOption.retainAll(options)).containsExactlyInAnyOrder(option2, option4);
    }

    @Test
    void removeAll() {
        // When
        var option1 = StandardOpenOption.CREATE;
        var option2 = S3OpenOption.preventConcurrentOverwrite();
        var option3 = StandardOpenOption.WRITE;
        var option4 = S3OpenOption.putOnlyIfModified();
        var options = Set.of(option1, option2, option3, option4);

        // Then
        then(S3OpenOption.removeAll(options)).containsExactlyInAnyOrder(option1, option3);
    }

    @Test
    void apply_GetObjectRequest_DoesNotModifyByDefault() {
        // Given
        S3OpenOption option = new TestS3OpenOption();
        GetObjectRequest.Builder builder = mock(GetObjectRequest.Builder.class);

        // When
        option.apply(builder);

        // Then
        verifyNoInteractions(builder);
    }

    @Test
    void apply_PutObjectRequest_DoesNotModifyByDefault() {
        // Given
        S3OpenOption option = new TestS3OpenOption();
        PutObjectRequest.Builder builder = mock(PutObjectRequest.Builder.class);
        Path file = mock(Path.class);

        // When
        option.apply(builder, file);

        // Then
        verifyNoInteractions(builder);
        verifyNoInteractions(file);
    }

    @Test
    void consume_GetObjectResponse_DoesNothingByDefault() {
        // Given
        S3OpenOption option = new TestS3OpenOption();
        GetObjectResponse response = mock(GetObjectResponse.class);
        Path file = mock(Path.class);

        // When
        option.consume(response, file);

        // Then
        verifyNoInteractions(response);
        verifyNoInteractions(file);
    }

    @Test
    void consume_PutObjectResponse_DoesNothingByDefault() {
        // Given
        S3OpenOption option = new TestS3OpenOption();
        PutObjectResponse response = mock(PutObjectResponse.class);
        Path file = mock(Path.class);

        // When
        option.consume(response, file);

        // Then
        verifyNoInteractions(response);
        verifyNoInteractions(file);
    }

    // Test implementation of S3OpenOption
    private static class TestS3OpenOption extends S3OpenOption {
        // implementation for testing default behaviors

        @Override
        public S3OpenOption newInstance() {
            throw new UnsupportedOperationException();
        }
    }
}
