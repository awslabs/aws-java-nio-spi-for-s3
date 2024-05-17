/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@DisplayName("S3TransferUtil")
class S3TransferUtilTest {

    @Test
    @DisplayName("upload should succeed")
    void uploadFileCompletesSuccessfully() throws IOException {
        S3Path file = mock();
        when(file.bucketName()).thenReturn("a");
        when(file.getKey()).thenReturn("a");

        final S3AsyncClient client = mock();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(completedFuture(PutObjectResponse.builder().build()));

        var util = new S3TransferUtil(client, 1L, TimeUnit.MINUTES);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.uploadLocalFile(file, tmpFile)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("IOException is thrown when timeout happens while uploading")
    void uploadTimeoutYieldsIOException() throws IOException {
        S3Path file = mock();
        when(file.bucketName()).thenReturn("a");
        when(file.getKey()).thenReturn("a");

        final S3AsyncClient client = mock();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(
            CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(Duration.ofMinutes(1).toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return PutObjectResponse.builder().build();
            })
        );

        var util = new S3TransferUtil(client, 1L, TimeUnit.MILLISECONDS);
        var tmpFile = Files.createTempFile(null, null);
        assertThatThrownBy(() -> util.uploadLocalFile(file, tmpFile))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(TimeoutException.class);
    }
}