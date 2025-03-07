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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3WritableByteChannelTest {

    @Test
    @DisplayName("when file exists and constructor is invoked with option `CREATE_NEW` should throw FileAlreadyExistsException")
    void whenFileExistsAndCreateNewShouldThrowFileAlreadyExistsException() throws InterruptedException, TimeoutException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(true);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);

        var file = S3Path.getPath(fs, "somefile");
        assertThatThrownBy(() -> new S3WritableByteChannel(file, mock(), mock(), Set.of(CREATE_NEW)))
                .isInstanceOf(FileAlreadyExistsException.class);
    }

    @Test
    @DisplayName("when file does not exist and constructor is invoked without option `CREATE_NEW` nor `CREATE` should throw NoSuchFileException")
    void whenFileDoesNotExistsAndNoCreateNewShouldThrowNoSuchFileException() throws InterruptedException, TimeoutException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(false);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);

        var file = S3Path.getPath(fs, "somefile");
        assertThatThrownBy(() -> new S3WritableByteChannel(file, mock(), mock(), emptySet()))
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    @DisplayName("S3WritableByteChannel is a SeekableByteChannel")
    void shouldBeSeekable() throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(true);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);

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
        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        var file = S3Path.getPath(fs, "somefile");

        var channel = new S3WritableByteChannel(file, mock(), mock(), Set.of(CREATE));

        var countAfterOpening = countTemporaryFiles(tempDir);
        channel.close();
        var countAfterClosing = countTemporaryFiles(tempDir);
        assertThat(countAfterClosing).isLessThan(countAfterOpening);
    }

    @Test
    @DisplayName("second close() call should be a no-op")
    void secondCloseIsNoOp() throws InterruptedException, TimeoutException, IOException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(S3AsyncClient.class), any())).thenReturn(false);
        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        var file = S3Path.getPath(fs, "somefile");

        S3TransferUtil utilMock = mock();
        var channel = new S3WritableByteChannel(file, mock(), utilMock, Set.of(CREATE));
        channel.close();
        // this close() call should be a no-op
        channel.close();

        verify(utilMock, times(1)).uploadLocalFile(any(), any());
    }

    private long countTemporaryFiles(Path tempDir) throws IOException {
        try (var list = Files.list(tempDir.getParent())) {
            return list
                    .filter((path) -> path.getFileName().toString().contains("aws-s3-nio-"))
                    .count();
        }
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