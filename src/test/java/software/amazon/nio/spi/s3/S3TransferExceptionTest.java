/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.spi.FileSystemProvider;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3TransferExceptionTest {

    @Test
    void test_cause() {
        var fs = mock(S3FileSystem.class);
        var provider = mock(FileSystemProvider.class);
        when(fs.provider()).thenReturn(provider);
        when(provider.getScheme()).thenReturn("s3");
        var path = S3Path.getPath(fs, "somefile");
        var cause = S3Exception.builder()
            .awsErrorDetails(AwsErrorDetails.builder()
                .errorCode("PreconditionFailed")
                .errorMessage("At least one of the pre-conditions you specified did not hold")
                .build())
            .numAttempts(1)
            .requestId("some-request")
            .statusCode(412)
            .build();
        var exception = new S3TransferException("HeadObject", path, cause);
        assertThat(exception).hasMessage("HeadObject => 412; somefile; At least one of the pre-conditions you specified did not hold");
        assertThat(exception.errorCode()).isEqualTo("PreconditionFailed");
        assertThat(exception.errorMessage()).isEqualTo("At least one of the pre-conditions you specified did not hold");
        assertThat(exception.numAttempts()).isEqualTo(1);
        assertThat(exception.requestId()).isEqualTo("some-request");
        assertThat(exception.statusCode()).isEqualTo(412);
    }

    @Test
    void test_emptyCause() {
        var fs = mock(S3FileSystem.class);
        var provider = mock(FileSystemProvider.class);
        when(fs.provider()).thenReturn(provider);
        when(provider.getScheme()).thenReturn("s3");
        var path = S3Path.getPath(fs, "somefile");
        var cause = S3Exception.builder().build();
        var exception = new S3TransferException("HeadObject", path, cause);
        assertThat(exception).hasMessage("HeadObject => 0; somefile");
        assertThat(exception.errorCode()).isEmpty();
        assertThat(exception.errorMessage()).isEmpty();
        assertThat(exception.numAttempts()).isNull();
        assertThat(exception.requestId()).isNull();
        assertThat(exception.statusCode()).isEqualTo(0);
    }
}
