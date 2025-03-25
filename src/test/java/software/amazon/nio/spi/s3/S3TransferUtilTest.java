/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.nio.file.StandardOpenOption.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@DisplayName("S3TransferUtil")
@SuppressWarnings("unchecked")
class S3TransferUtilTest {

    @Test
    @DisplayName("download should succeed (with open options)")
    void downloadFileCompletesSuccessfully_withOpenOption() throws IOException {
        var file = mock(S3Path.class);

        var client = mock(S3AsyncClient.class);
        var responseFuture = completedFuture(GetObjectResponse.builder().build());
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        var option1 = mock(S3OpenOption.class);
        var option2 = mock(S3OpenOption.class);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of(option1, option2))).doesNotThrowAnyException();
        verify(option1, times(1)).apply(any(GetObjectRequest.Builder.class));
        verify(option1, times(1)).consume(any(GetObjectResponse.class));
        verify(option2, times(1)).apply(any(GetObjectRequest.Builder.class));
        verify(option2, times(1)).consume(any(GetObjectResponse.class));
    }

    @Test
    @DisplayName("download should succeed (without timeout)")
    void downloadFileCompletesSuccessfully_withoutTimeout() throws IOException {
        var file = mock(S3Path.class);

        var client = mock(S3AsyncClient.class);
        var responseFuture = completedFuture(GetObjectResponse.builder().build());
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("download should succeed (with timeout)")
    void downloadFileCompletesSuccessfully_withTimeout() throws IOException {
        var file = mock(S3Path.class);

        var client = mock(S3AsyncClient.class);
        var responseFuture = completedFuture(GetObjectResponse.builder().build());
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, 1L, TimeUnit.MINUTES, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("IOException is thrown when download failed for unknown reason")
    void downloadFileFailsDueForUnknownReason() throws IOException {
        var file = mock(S3Path.class);
        when(file.toString()).thenReturn("somefile");

        var client = mock(S3AsyncClient.class);
        var exception = new CompletionException(new RuntimeException("unknown error"));
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenThrow(exception);

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(IOException.class)
            .hasMessage("Could not read from path: somefile")
            .hasCause(exception);
    }

    @Test
    @DisplayName("IOException is thrown when download failed with 400")
    void downloadFileFailsDueTo400() throws IOException {
        var file = mock(S3Path.class);
        when(file.toString()).thenReturn("somefile");

        var client = mock(S3AsyncClient.class);
        var exception = new CompletionException(S3Exception.builder().statusCode(400).build());
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenThrow(exception);

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(S3TransferException.class)
            .hasMessage("GetObject => 400; somefile")
            .hasCause(exception.getCause());
    }

    @Test
    @DisplayName("IOException is thrown when download failed with 404")
    void downloadFileFailsDueTo404() throws IOException {
        var file = mock(S3Path.class);
        when(file.toString()).thenReturn("somefile");

        var client = mock(S3AsyncClient.class);
        var exception = new CompletionException(S3Exception.builder().statusCode(404).build());
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenThrow(exception);

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(NoSuchFileException.class)
            .hasMessage("somefile");
    }

    @Test
    @DisplayName("when CREATE open option is present, a failed download with a 404 status code is gracefully handled")
    void downloadFileFailsDueTo404ButCreateOption() throws IOException {
        var file = mock(S3Path.class);

        var client = mock(S3AsyncClient.class);
        var exception = new CompletionException(S3Exception.builder().statusCode(404).build());
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenThrow(exception);

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of(CREATE))).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("IOException is thrown when timeout happens while downloading")
    void downloadFileFailsDueToTimeout() throws Exception {
        var file = mock(S3Path.class);
        when(file.toString()).thenReturn("somefile");

        var client = mock(S3AsyncClient.class);
        var exception = new TimeoutException("test timeout");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.MINUTES)).thenThrow(exception);
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, 1L, TimeUnit.MINUTES, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(IOException.class)
            .hasMessage("Could not read from path: somefile")
            .hasCause(exception);
    }

    @Test
    @DisplayName("IOException is thrown when interrupted while downloading")
    void downloadFileInterrupted() throws Exception {
        var file = mock(S3Path.class);
        when(file.toString()).thenReturn("somefile");

        var client = mock(S3AsyncClient.class);
        var exception = new InterruptedException("test interruption");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.MINUTES)).thenThrow(exception);
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, 1L, TimeUnit.MINUTES, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.downloadToLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(IOException.class)
            .hasMessage("Could not read from path: somefile")
            .hasCause(exception);
    }

    @Test
    @DisplayName("upload should succeed (with open options)")
    void uploadFileCompletesSuccessfully_withOpenOption() throws IOException {
        var file = mock(S3Path.class);
        when(file.bucketName()).thenReturn("a");
        when(file.getKey()).thenReturn("a");

        var client = mock(S3AsyncClient.class);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(completedFuture(PutObjectResponse.builder().build()));

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        var option1 = mock(S3OpenOption.class);
        var option2 = mock(S3OpenOption.class);
        assertThatCode(() -> util.uploadLocalFile(file, tmpFile, Set.of(option1, option2))).doesNotThrowAnyException();
        verify(option1, times(1)).apply(any(PutObjectRequest.Builder.class));
        verify(option1, times(1)).consume(any(PutObjectResponse.class));
        verify(option2, times(1)).apply(any(PutObjectRequest.Builder.class));
        verify(option2, times(1)).consume(any(PutObjectResponse.class));
    }

    @Test
    @DisplayName("upload should succeed (without timeout spec)")
    void uploadFileCompletesSuccessfully_withoutTimeout() throws IOException {
        S3Path file = mock();
        when(file.bucketName()).thenReturn("a");
        when(file.getKey()).thenReturn("a");

        final S3AsyncClient client = mock();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(completedFuture(PutObjectResponse.builder().build()));

        var util = new S3TransferUtil(client, null, null, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.uploadLocalFile(file, tmpFile, Set.of())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("upload should succeed (with timeout spec)")
    void uploadFileCompletesSuccessfully_withTimeout() throws IOException {
        S3Path file = mock();
        when(file.bucketName()).thenReturn("a");
        when(file.getKey()).thenReturn("a");

        final S3AsyncClient client = mock();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(completedFuture(PutObjectResponse.builder().build()));

        var util = new S3TransferUtil(client, 1L, TimeUnit.MINUTES, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.uploadLocalFile(file, tmpFile, Set.of())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("IOException is thrown when interrupted while uploading")
    void uploadFileFailedDueTo400() throws Exception {
        var file = mock(S3Path.class);
        when(file.toString()).thenReturn("somefile");

        var client = mock(S3AsyncClient.class);
        var exception = new CompletionException(S3Exception.builder().statusCode(400).build());
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.SECONDS)).thenThrow(exception);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, 1L, TimeUnit.SECONDS, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.uploadLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(S3TransferException.class)
            .hasMessage("PutObject => 400; somefile")
            .hasCause(exception.getCause());
    }

    @Test
    @DisplayName("IOException is thrown when interrupted while uploading")
    void uploadFileInterrupted() throws Exception {
        var file = mock(S3Path.class);
        when(file.toString()).thenReturn("somefile");

        var client = mock(S3AsyncClient.class);
        var exception = new InterruptedException("test interruption");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.SECONDS)).thenThrow(exception);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, 1L, TimeUnit.SECONDS, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.uploadLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(IOException.class)
            .hasMessage("Could not write to path: somefile")
            .hasCause(exception);
    }

    @Test
    @DisplayName("IOException is thrown when timeout happens while uploading")
    void uploadTimeoutYieldsIOException() throws Exception {
        S3Path file = mock();
        when(file.bucketName()).thenReturn("a");
        when(file.getKey()).thenReturn("a");

        var client = mock(S3AsyncClient.class);
        var exception = new TimeoutException("test timeout");
        var responseFuture = mock(CompletableFuture.class);
        when(responseFuture.get(1L, TimeUnit.MILLISECONDS)).thenThrow(exception);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(responseFuture);

        var util = new S3TransferUtil(client, 1L, TimeUnit.MILLISECONDS, DisabledFileIntegrityCheck.INSTANCE);
        var tmpFile = Files.createTempFile(null, null);
        assertThatThrownBy(() -> util.uploadLocalFile(file, tmpFile, Set.of()))
            .isInstanceOf(IOException.class)
            .hasCauseInstanceOf(TimeoutException.class);
    }
}
