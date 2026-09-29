/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class S3RangeHeaderTest {

    @Test
    void constructor_WithValidRange_CreatesHeader() {
        // Given
        int start = 0;
        int end = 100;
        
        // When
        S3RangeHeader header = new S3RangeHeader(start, end);
        
        // Then
        GetObjectRequest.Builder builder = GetObjectRequest.builder();
        header.apply(builder);
        
        GetObjectRequest request = builder.build();
        assertThat(request.range()).isEqualTo("bytes=0-100");
    }

    @ParameterizedTest
    @CsvSource({
        "0, 0",     // Single byte
        "100, 200", // Normal range
        "0, 1000",  // Larger range
        "1000, 1000" // Same start and end
    })
    void constructor_WithVariousValidRanges_CreatesCorrectHeaders(int start, int end) {
        // When
        S3RangeHeader header = new S3RangeHeader(start, end);
        
        // Then
        GetObjectRequest.Builder builder = GetObjectRequest.builder();
        header.apply(builder);
        
        GetObjectRequest request = builder.build();
        assertThat(request.range()).isEqualTo("bytes=" + start + "-" + end);
    }

    @Test
    void constructor_WithNegativeStart_ThrowsException() {
        assertThatThrownBy(() -> new S3RangeHeader(-1, 100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("start must be non-negative");
    }

    @Test
    void constructor_WithNegativeEnd_ThrowsException() {
        assertThatThrownBy(() -> new S3RangeHeader(0, -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("end must be non-negative");
    }

    @Test
    void apply_ModifiesBuilderWithRange() {
        // Given
        S3RangeHeader header = new S3RangeHeader(100, 200);
        GetObjectRequest.Builder mockBuilder = mock(GetObjectRequest.Builder.class);
        when(mockBuilder.range(anyString())).thenReturn(mockBuilder);

        // When
        header.apply(mockBuilder);

        // Then
        verify(mockBuilder).range("bytes=100-200");
    }

    @Test
    void inheritance_ExtendsS3OpenOption() {
        // Given
        S3RangeHeader header = new S3RangeHeader(0, 100);

        // Then
        assertThat(header).isInstanceOf(S3OpenOption.class);
    }
}
