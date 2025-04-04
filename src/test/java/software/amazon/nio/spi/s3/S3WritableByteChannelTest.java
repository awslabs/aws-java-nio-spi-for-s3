/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.BDDAssertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("unchecked")
class S3WritableByteChannelTest {

    @Test
    @DisplayName("when file exists and constructor is invoked with an S3 specific open option")
    void whenFileExistsAndS3OpenOptionIsApplied() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));
        var file = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenReturn(completedFuture(GetObjectResponse.builder().build()));
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(completedFuture(PutObjectResponse.builder().build()));
        var transferManager = new S3TransferUtil(client, null, null);

        var option1 =  mock(S3OpenOption.class);
        var option2 =  mock(S3OpenOption.class);
        try (var channel = new S3WritableByteChannel(file, client, transferManager, Set.of(CREATE, option1, option2))) {
            assertThat(channel.position()).isZero();
        }
        verify(option1, times(1)).apply(any(GetObjectRequest.Builder.class));
        verify(option2, times(1)).apply(any(GetObjectRequest.Builder.class));
        verify(client, times(1)).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
        verify(client, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        verifyNoMoreInteractions(client);
    }

    @Test
    @DisplayName("when file exists and constructor is invoked with option `CREATE_NEW` should throw FileAlreadyExistsException")
    void whenFileExistsAndCreateNewShouldThrowFileAlreadyExistsException() throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(true);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));

        var file = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        assertThatThrownBy(() -> new S3WritableByteChannel(file, client, mock(), Set.of(CREATE_NEW)))
                .isInstanceOf(FileAlreadyExistsException.class);
    }

    @Test
    @DisplayName("when file does not exist and constructor is invoked the option `CREATE` option")
    void whenFileDoesNotExistsAndCreateOptionIsPresent() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));
        var file = S3Path.getPath(fs, "somefile");
        var s3Client = mock(S3AsyncClient.class);
        var transferManager = new S3TransferUtil(s3Client, null, null);
        var exception = new CompletionException(S3Exception.builder().statusCode(404).build());
        when(s3Client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenThrow(exception);
        when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(completedFuture(PutObjectResponse.builder().build()));

        try (var channel = new S3WritableByteChannel(file, s3Client, transferManager, Set.of(CREATE))) {
            assertThat(channel.position()).isZero();
        }
    }

    @Test
    @DisplayName("when file does not exist and constructor is invoked without option `CREATE_NEW` nor `CREATE` should throw NoSuchFileException")
    void whenFileDoesNotExistsAndNoCreateShouldThrowNoSuchFileException() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));
        var file = S3Path.getPath(fs, "somefile");
        var s3Client = mock(S3AsyncClient.class);
        var transferManager = new S3TransferUtil(s3Client, null, null);
        var exception = new CompletionException(S3Exception.builder().statusCode(404).build());
        doThrow(exception).when(s3Client).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));

        assertThatThrownBy(() -> new S3WritableByteChannel(file, s3Client, transferManager, emptySet()))
            .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    @DisplayName("when file the download fails due to an invalid request")
    void whenFileDownloadFailsDueToInvalidRequestTheExceptionShouldBePropagated() throws InterruptedException, TimeoutException, ExecutionException, IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));
        var file = S3Path.getPath(fs, "somefile");
        var s3Client = mock(S3AsyncClient.class);
        var transferManager = new S3TransferUtil(s3Client, null, null);
        var exception = new CompletionException(S3Exception.builder().statusCode(400).message("Invalid Request").build());
        doThrow(exception).when(s3Client).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));

        assertThatThrownBy(() -> new S3WritableByteChannel(file, s3Client, transferManager, emptySet()))
            .isInstanceOf(S3TransferException.class)
            .hasMessage("GetObject => 400; somefile")
            .hasCause(exception.getCause());
    }

    @Test
    @DisplayName("when file the download fails for an unknown reason")
    void whenFileDownloadFailsForUnknownReasonTheExceptionShouldBePropagated() throws InterruptedException, TimeoutException, ExecutionException, IOException {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));
        var file = S3Path.getPath(fs, "somefile");
        var s3Client = mock(S3AsyncClient.class);
        var transferManager = new S3TransferUtil(s3Client, null, null);
        var exception = new CompletionException(new RuntimeException("unknown error"));
        doThrow(exception).when(s3Client).getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));

        assertThatThrownBy(() -> new S3WritableByteChannel(file, s3Client, transferManager, emptySet()))
            .isInstanceOf(IOException.class)
            .hasMessage("Could not read from path: somefile")
            .hasCause(exception);
    }

    @Test
    @DisplayName("when option `CREATE_NEW` is present an exists check is performed")
    void whenCreateNewOptionOptionIsPresentNoExistsCall() throws InterruptedException, TimeoutException, IOException {
        var provider = mock(S3FileSystemProvider.class);
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(true);
        var fs = mock(S3FileSystem.class);
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));

        var file = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        thenThrownBy(() -> new S3WritableByteChannel(file, client, mock(), Set.of(CREATE_NEW)))
            .isInstanceOf(FileAlreadyExistsException.class)
            .hasMessage(file.toString());
        verify(provider, times(1)).exists(any(), any());
    }

    @Test
    @DisplayName("when `assumeObjectNotExists` option is present no download should be performed")
    void whenAssumeObjectNotExistsOptionIsPresentNoGetObjectOnlyPutObject() throws Exception {
        var provider = mock(S3FileSystemProvider.class);
        var fs = mock(S3FileSystem.class);
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));
        var file = S3Path.getPath(fs, "somefile");
        var client = mock(S3AsyncClient.class);
        var transferManager = mock(S3TransferUtil.class);
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(completedFuture(PutObjectResponse.builder().build()));

        var assumeObjectNotExists = S3OpenOption.assumeObjectNotExists();
        try (var channel = new S3WritableByteChannel(file, client, transferManager, Set.of(assumeObjectNotExists))) {
            assertThat(channel.position()).isZero();
        }
        verify(transferManager, never()).downloadToLocalFile(any(), any(), any());
    }

    @Test
    @DisplayName("S3WritableByteChannel is a SeekableByteChannel")
    void shouldBeSeekable() throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(true);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));

        var file = S3Path.getPath(fs, "somefile");
        var channel = new S3WritableByteChannel(file, mock(), mock(), Set.of(READ, WRITE));
        assertThat(channel.size()).isZero();
        assertThat(channel.position()).isZero();
        channel.write(ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 }));
        assertThat(channel.position()).isEqualTo(4);
        assertThat(channel.position(2)).isSameAs(channel);
        channel.write(ByteBuffer.wrap(new byte[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }));
        assertThat(channel.size()).isEqualTo(12);
        assertThat(channel.position(3)).isSameAs(channel);
        ByteBuffer buffer = ByteBuffer.allocate(6);
        channel.read(buffer);
        assertThat(buffer.array()).contains(4, 5, 6, 7, 8, 9);
        assertThatThrownBy(() -> channel.truncate(6)).isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest(name = "can be instantiated when file exists ({0}) and open options are {1}")
    @MethodSource("acceptedFileExistsAndOpenOptions")
    @DisplayName("S3WritableByteChannel")
    void shouldNotThrowWhen(boolean fileExists, Set<StandardOpenOption> openOptions) throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(fileExists);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));

        var file = S3Path.getPath(fs, "somefile");
        new S3WritableByteChannel(file, mock(), mock(), openOptions).close();
    }

    @Test
    @DisplayName("open() should be true before close()")
    void shouldBeOpenBeforeClose() throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(false);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));

        var file = S3Path.getPath(fs, "somefile");
        try(var channel = new S3WritableByteChannel(file, mock(), mock(), Set.of(CREATE))){
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    @DisplayName("open() should be false after close()")
    void shouldBeNotOpenAfterClose() throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(false);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));

        var file = S3Path.getPath(fs, "somefile");
        var channel = new S3WritableByteChannel(file, mock(), mock(), Set.of(CREATE));
        channel.close();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    @DisplayName("close() should clean up the temporary file")
    void tmpFileIsCleanedUpAfterClose(@TempDir Path tempDir) throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(false);
        var fs = new S3FileSystem(provider, null, tempDir);
        var file1 = S3Path.getPath(fs, "file1");
        var file2 = S3Path.getPath(fs, "dir1/file2");
        var file3 = S3Path.getPath(fs, "dir1/dir2/file3");

        var channel1 = new S3WritableByteChannel(file1, mock(), mock(), Set.of(CREATE));
        var channel2 = new S3WritableByteChannel(file2, mock(), mock(), Set.of(CREATE));
        var channel3 = new S3WritableByteChannel(file3, mock(), mock(), Set.of(CREATE));

        assertThat(countTemporaryFiles(tempDir)).isEqualTo(3);
        channel1.close();
        channel2.close();
        channel3.close();
        assertThat(countTemporaryFiles(tempDir)).isZero();
    }

    @Test
    @DisplayName("second close() call should be a no-op")
    void secondCloseIsNoOp() throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(false);
        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        when(fs.createTempFile(any(S3Path.class))).thenReturn(Files.createTempFile("", ""));
        var file = S3Path.getPath(fs, "somefile");

        S3TransferUtil utilMock = mock();
        var channel = new S3WritableByteChannel(file, mock(), utilMock, Set.of(CREATE));
        channel.close();
        // this close() call should be a no-op
        channel.close();

        verify(utilMock, times(1)).uploadLocalFile(any(), any(), any());
    }

    private long countTemporaryFiles(Path tempDir) throws IOException {
        var visitor = new SimpleFileVisitor<Path>() {
            int fileCount = 0;

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                fileCount++;
                return super.visitFile(file, attrs);
            }
        };
        Files.walkFileTree(tempDir, visitor);
        return visitor.fileCount;
    }

    private Stream<Arguments> acceptedFileExistsAndOpenOptions() {
        return Stream.of(
            Arguments.of(false, Set.of(CREATE)),
            Arguments.of(false, Set.of(CREATE_NEW)),
            Arguments.of(false, Set.of(CREATE, CREATE_NEW)),
            Arguments.of(true, Set.of(CREATE))
        );
    }
}