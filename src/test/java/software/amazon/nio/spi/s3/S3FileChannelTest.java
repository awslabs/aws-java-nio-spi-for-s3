/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class S3FileChannelTest {

    @Mock
    private S3SeekableByteChannel s3SeekableByteChannel;
    private S3FileChannel s3FileChannel;
    private ByteBuffer s3FileChannelBuffer;

    private final byte[] testBytes = "ABCDEFGHIJ".getBytes();

    @BeforeEach
    public void init() throws IOException {
        s3FileChannel = new S3FileChannel(s3SeekableByteChannel);
        s3FileChannelBuffer = ByteBuffer.wrap(testBytes);

    }

    private void setupReadMocks() throws IOException {
        // this mocks the population of a destBuffer by the s3SeekableByteChannel returning the length read followed by -1
        // indicating no more data
        when(s3SeekableByteChannel.read(any(ByteBuffer.class)))
                .then(invocationOnMock -> {
                    if  (s3FileChannelBuffer.remaining() == 0) {
                        return -1;
                    }

                    ByteBuffer buffer = invocationOnMock.getArgument(0);
                    byte[] bytes = new byte[Math.min(buffer.remaining(), s3FileChannelBuffer.remaining())];
                    s3FileChannelBuffer.get(bytes);
                    buffer.put(bytes).flip();

                    return bytes.length;
                });
    }

    @Test
    public void testRead() throws IOException {
        setupReadMocks();
        ByteBuffer dest  = ByteBuffer.allocate(10);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(bytesRead, 10);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
    public void testReadLessBytes() throws IOException {
        setupReadMocks();
        ByteBuffer dest  = ByteBuffer.allocate(5);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(bytesRead, 5);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
    public void testReadMoreBytes() throws IOException {
        setupReadMocks();
        ByteBuffer dest  = ByteBuffer.allocate(15);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(bytesRead, testBytes.length);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test()
    public void testScatterRead() throws IOException {
        setupReadMocks();
        ByteBuffer[] buffers = {ByteBuffer.allocate(5), ByteBuffer.allocate(7)};
        long bytesRead = s3FileChannel.read(buffers, 0, buffers.length);
        assertEquals(10, bytesRead);
        assertEquals(5, buffers[0].limit());
        assertEquals(5, buffers[1].limit());
        for (int i = 0; i < 5; i++) {
            assertEquals(buffers[0].get(i), testBytes[i]);
            assertEquals(buffers[1].get(i), testBytes[i+5]);
        }
    }

    @Test
    public void testReadStartingAt() throws IOException {
        setupReadMocks();
        ByteBuffer dest  = ByteBuffer.allocate(10);

        s3FileChannel.read(dest, 6L);

        // verify delegation by s3FileChannel
        verify(s3SeekableByteChannel).position(6L);
        verify(s3SeekableByteChannel).read(dest);
    }

    @Test
    public void testReadStartingAtNegativeNumber() throws IOException {
        ByteBuffer dest  = ByteBuffer.allocate(10);
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> s3FileChannel.read(dest, -6L),
                "file position must be non-negative");
    }

    @Test
    public void testPositionWithLong() throws IOException {
        // verify delegation by s3FileChannel
        s3FileChannel.position(5L);
        verify(s3SeekableByteChannel).position(5L);
        verifyNoMoreInteractions(s3SeekableByteChannel);
    }

    @Test
    public void testPosition() throws IOException {
        // verify delegation by s3FileChannel
        s3FileChannel.position();
        verify(s3SeekableByteChannel).position();
        verifyNoMoreInteractions(s3SeekableByteChannel);
    }

    @Test
    public void testSize() throws IOException {
        // verify delegation by s3FileChannel
        s3FileChannel.size();
        verify(s3SeekableByteChannel).size();
        verifyNoMoreInteractions(s3SeekableByteChannel);
    }

    @Test
    public void testClose() throws IOException {
        // verify delegation by s3FileChannel
        s3FileChannel.close();
        verify(s3SeekableByteChannel).close();
        verifyNoMoreInteractions(s3SeekableByteChannel);
    }

    @Test
    public void testMap() throws IOException {
        assertThrowsExactly(
                IOException.class,
                () -> s3FileChannel.map(FileChannel.MapMode.READ_ONLY, 0, 0),
                "This library current doesn't support MappedByteBuffers");
    }

    @Test
    public void testLock() {
        assertThrowsExactly(
                IOException.class,
                () -> s3FileChannel.lock(),
                "S3 does not support file locks");
    }
}
