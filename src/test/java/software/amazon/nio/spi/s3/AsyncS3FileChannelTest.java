/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AsyncS3FileChannelTest {

    @Mock
    private S3SeekableByteChannel mockByteChannel;

    @Mock
    private S3WritableByteChannel mockWriteDelegate;

    private AsyncS3FileChannel channel;

    @BeforeEach
    void setUp() {
        channel = new AsyncS3FileChannel(mockByteChannel);
    }

    @Test
    void size_DelegatesToByteChannel() throws IOException {
        // Given
        when(mockByteChannel.size()).thenReturn(1000L);

        // When
        long size = channel.size();

        // Then
        assertThat(size).isEqualTo(1000L);
        verify(mockByteChannel).size();
    }

    @Test
    void truncate_DelegatesToByteChannel() throws IOException {
        // Given
        when(mockByteChannel.truncate(anyLong())).thenReturn(mockByteChannel);

        // When
        AsyncS3FileChannel result = (AsyncS3FileChannel) channel.truncate(500L);

        // Then
        assertThat(result).isSameAs(channel);
        verify(mockByteChannel).truncate(500L);
    }

    @Test
    void force_WithWriteDelegate_CallsForce() throws IOException {
        // Given
        when(mockByteChannel.getWriteDelegate()).thenReturn(mockWriteDelegate);

        // When
        channel.force(true);

        // Then
        verify(mockWriteDelegate).force();
    }

    @Test
    void force_WithoutWriteDelegate_DoesNothing() throws IOException {
        // Given
        when(mockByteChannel.getWriteDelegate()).thenReturn(null);

        // When
        channel.force(true);

        // Then
        verifyNoInteractions(mockWriteDelegate);
    }

    @Test
    void lock_WithCompletionHandler_ThrowsUnsupportedOperationException() {
        // Given
        @SuppressWarnings("unchecked")
        CompletionHandler<FileLock, Object> handler = mock(CompletionHandler.class);
        Object attachment = new Object();

        // When/Then
        assertThatThrownBy(() -> channel.lock(0, 100, false, attachment, handler))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("S3 does not support file locking");
    }

    @Test
    void lock_ReturnsFailedFuture() throws ExecutionException, InterruptedException, TimeoutException {
        // When
        Future<FileLock> future = channel.lock(0, 100, false);

        // Then
        assertThat(future.isDone()).isTrue();
        assertThatThrownBy(() -> future.get(100, TimeUnit.MILLISECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("S3 does not support file locking");
    }

    @Test
    void tryLock_ThrowsIOException() {
        // When/Then
        assertThatThrownBy(() -> channel.tryLock(0, 100, false))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("S3 does not support file locking");
    }

    @Test
    void read_WithCompletionHandler_Success() throws IOException {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);
        @SuppressWarnings("unchecked")
        CompletionHandler<Integer, String> handler = mock(CompletionHandler.class);
        String attachment = "test-attachment";

        when(mockByteChannel.read(any(ByteBuffer.class))).thenReturn(50);

        // When
        channel.read(buffer, 100L, attachment, handler);

        // Then
        verify(mockByteChannel).position(100L);
        verify(mockByteChannel).read(buffer);
        verify(handler).completed(50, attachment);
    }

    @Test
    void read_WithCompletionHandler_Failure() throws IOException {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);
        @SuppressWarnings("unchecked")
        CompletionHandler<Integer, String> handler = mock(CompletionHandler.class);
        String attachment = "test-attachment";
        IOException exception = new IOException("Test exception");

        when(mockByteChannel.read(any(ByteBuffer.class))).thenThrow(exception);

        // When
        channel.read(buffer, 100L, attachment, handler);

        // Then
        verify(mockByteChannel).position(100L);
        verify(handler).failed(any(Exception.class), eq(attachment));
    }

    @Test
    void read_WithFuture_Success() throws Exception {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);
        when(mockByteChannel.read(any(ByteBuffer.class))).thenReturn(50);

        // When
        Future<Integer> future = channel.read(buffer, 100L);
        Integer result = future.get(1, TimeUnit.SECONDS);

        // Then
        assertThat(result).isEqualTo(50);
        verify(mockByteChannel).position(100L);
        verify(mockByteChannel).read(buffer);
    }

    @Test
    void read_WithNegativePosition_ThrowsIllegalArgumentException() {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);

        // When/Then
        assertThatThrownBy(() -> channel.read(buffer, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("position: -1");
    }

    @Test
    void write_WithCompletionHandler_Success() throws IOException {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);
        buffer.put(new byte[50]);
        buffer.flip();
        @SuppressWarnings("unchecked")
        CompletionHandler<Integer, String> handler = mock(CompletionHandler.class);
        String attachment = "test-attachment";

        when(mockByteChannel.write(any(ByteBuffer.class))).thenReturn(50);

        // When
        channel.write(buffer, 100L, attachment, handler);

        // Then
        verify(mockByteChannel).position(100L);
        verify(mockByteChannel).write(buffer);
        verify(handler).completed(50, attachment);
    }

    @Test
    void write_WithCompletionHandler_Failure() throws IOException {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);
        @SuppressWarnings("unchecked")
        CompletionHandler<Integer, String> handler = mock(CompletionHandler.class);
        String data = "test-data";
        IOException exception = new IOException("Test exception");

        when(mockByteChannel.write(any(ByteBuffer.class))).thenThrow(exception);

        // When
        channel.write(buffer, 100L, data, handler);

        // Then
        verify(mockByteChannel).position(100L);
        verify(handler).failed(any(Exception.class), eq(data));
    }

    @Test
    void write_WithFuture_Success() throws Exception {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);
        buffer.put(new byte[50]);
        buffer.flip();
        when(mockByteChannel.write(any(ByteBuffer.class))).thenReturn(50);

        // When
        Future<Integer> future = channel.write(buffer, 100L);
        Integer result = future.get(1, TimeUnit.SECONDS);

        // Then
        assertThat(result).isEqualTo(50);
        verify(mockByteChannel).position(100L);
        verify(mockByteChannel).write(buffer);
    }

    @Test
    void write_WithNegativePosition_ThrowsIllegalArgumentException() {
        // Given
        ByteBuffer buffer = ByteBuffer.allocate(100);

        // When/Then
        assertThatThrownBy(() -> channel.write(buffer, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("position: -1");
    }

    @Test
    void isOpen_DelegatesToByteChannel() {
        // Given
        when(mockByteChannel.isOpen()).thenReturn(true);

        // When
        boolean isOpen = channel.isOpen();

        // Then
        assertThat(isOpen).isTrue();
        verify(mockByteChannel).isOpen();
    }

    @Test
    void close_DelegatesToByteChannel() throws IOException {
        // When
        channel.close();

        // Then
        verify(mockByteChannel).close();
    }
}
