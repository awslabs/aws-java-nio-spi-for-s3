/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.OpenOption;
import org.junit.jupiter.api.Test;

class S3StreamingMultipartUploadTest {

    @Test
    void streamingMultipartUpload_defaultFactory_returnsInstanceWithDefaults() {
        S3OpenOption option = S3OpenOption.streamingMultipartUpload();

        assertThat(option)
            .isInstanceOf(S3StreamingMultipartUpload.class)
            .isInstanceOf(OpenOption.class);

        S3StreamingMultipartUpload streaming = (S3StreamingMultipartUpload) option;
        assertThat(streaming.getPartSize()).isEqualTo(8 * 1024 * 1024L);
        assertThat(streaming.getMaxInFlight()).isEqualTo(4);
    }

    @Test
    void streamingMultipartUpload_customPartSize_returnsInstanceWithCustomSize() {
        long customPartSize = 16 * 1024 * 1024L; // 16 MiB
        S3OpenOption option = S3OpenOption.streamingMultipartUpload(customPartSize);

        assertThat(option).isInstanceOf(S3StreamingMultipartUpload.class);

        S3StreamingMultipartUpload streaming = (S3StreamingMultipartUpload) option;
        assertThat(streaming.getPartSize()).isEqualTo(customPartSize);
        assertThat(streaming.getMaxInFlight()).isEqualTo(4);
    }

    @Test
    void streamingMultipartUpload_minimumPartSize_accepted() {
        long minPartSize = 5 * 1024 * 1024L; // 5 MiB
        S3OpenOption option = S3OpenOption.streamingMultipartUpload(minPartSize);

        S3StreamingMultipartUpload streaming = (S3StreamingMultipartUpload) option;
        assertThat(streaming.getPartSize()).isEqualTo(minPartSize);
    }

    @Test
    void streamingMultipartUpload_maximumPartSize_accepted() {
        long maxPartSize = 5L * 1024 * 1024 * 1024; // 5 GiB
        S3OpenOption option = S3OpenOption.streamingMultipartUpload(maxPartSize);

        S3StreamingMultipartUpload streaming = (S3StreamingMultipartUpload) option;
        assertThat(streaming.getPartSize()).isEqualTo(maxPartSize);
    }

    @Test
    void streamingMultipartUpload_partSizeBelowMinimum_throwsIllegalArgumentException() {
        long tooSmall = 5 * 1024 * 1024L - 1; // 5 MiB - 1 byte

        assertThatThrownBy(() -> S3OpenOption.streamingMultipartUpload(tooSmall))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Part size must be at least 5 MiB")
            .hasMessageContaining(String.valueOf(tooSmall));
    }

    @Test
    void streamingMultipartUpload_partSizeAboveMaximum_throwsIllegalArgumentException() {
        long tooLarge = 5L * 1024 * 1024 * 1024 + 1; // 5 GiB + 1 byte

        assertThatThrownBy(() -> S3OpenOption.streamingMultipartUpload(tooLarge))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Part size must not exceed 5 GiB")
            .hasMessageContaining(String.valueOf(tooLarge));
    }

    @Test
    void streamingMultipartUpload_zeroPartSize_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> S3OpenOption.streamingMultipartUpload(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Part size must be at least 5 MiB");
    }

    @Test
    void streamingMultipartUpload_negativePartSize_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> S3OpenOption.streamingMultipartUpload(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Part size must be at least 5 MiB");
    }

    @Test
    void copy_returnsNewInstanceWithSameConfiguration() {
        S3StreamingMultipartUpload original = new S3StreamingMultipartUpload(
            16 * 1024 * 1024L, 8);

        S3OpenOption copied = original.copy();

        assertThat(copied)
            .isInstanceOf(S3StreamingMultipartUpload.class)
            .isNotSameAs(original);

        S3StreamingMultipartUpload copiedStreaming = (S3StreamingMultipartUpload) copied;
        assertThat(copiedStreaming.getPartSize()).isEqualTo(original.getPartSize());
        assertThat(copiedStreaming.getMaxInFlight()).isEqualTo(original.getMaxInFlight());
    }

    @Test
    void copy_defaultInstance_returnsNewInstanceWithDefaults() {
        S3OpenOption original = S3OpenOption.streamingMultipartUpload();

        S3OpenOption copied = ((S3StreamingMultipartUpload) original).copy();

        assertThat(copied).isNotSameAs(original);

        S3StreamingMultipartUpload copiedStreaming = (S3StreamingMultipartUpload) copied;
        assertThat(copiedStreaming.getPartSize()).isEqualTo(S3StreamingMultipartUpload.DEFAULT_PART_SIZE);
        assertThat(copiedStreaming.getMaxInFlight()).isEqualTo(S3StreamingMultipartUpload.DEFAULT_MAX_IN_FLIGHT);
    }

    @Test
    void constants_haveCorrectValues() {
        assertThat(S3StreamingMultipartUpload.MIN_PART_SIZE).isEqualTo(5 * 1024 * 1024L);
        assertThat(S3StreamingMultipartUpload.MAX_PART_SIZE).isEqualTo(5L * 1024 * 1024 * 1024);
        assertThat(S3StreamingMultipartUpload.DEFAULT_PART_SIZE).isEqualTo(8 * 1024 * 1024L);
        assertThat(S3StreamingMultipartUpload.MAX_PARTS).isEqualTo(10_000);
        assertThat(S3StreamingMultipartUpload.DEFAULT_MAX_IN_FLIGHT).isEqualTo(4);
    }
}
