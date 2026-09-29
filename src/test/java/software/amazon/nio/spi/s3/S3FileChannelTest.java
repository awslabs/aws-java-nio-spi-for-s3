/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class S3FileChannelTest {

    @Mock
    private S3SeekableByteChannel s3SeekableByteChannel;
    
    @Mock
    private S3WritableByteChannel writeDelegate;
    
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
    
    private void setupWriteMocks() throws IOException {
        when(s3SeekableByteChannel.write(any(ByteBuffer.class)))
                .then(invocationOnMock -> {
                    ByteBuffer buffer = invocationOnMock.getArgument(0);
                    int bytesToWrite = buffer.remaining();
                    buffer.position(buffer.position() + bytesToWrite);
                    return bytesToWrite;
                });
    }

    @Test
    public void testRead() throws IOException {
        setupReadMocks();
        ByteBuffer dest = ByteBuffer.allocate(10);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(10, bytesRead);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
    public void testReadLessBytes() throws IOException {
        setupReadMocks();
        ByteBuffer dest = ByteBuffer.allocate(5);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(5, bytesRead);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
    public void testReadMoreBytes() throws IOException {
        setupReadMocks();
        ByteBuffer dest = ByteBuffer.allocate(15);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(bytesRead, testBytes.length);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
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
    public void testScatterReadWithOffset() throws IOException {
        setupReadMocks();
        ByteBuffer[] buffers = {ByteBuffer.allocate(2), ByteBuffer.allocate(5), ByteBuffer.allocate(7)};
        long bytesRead = s3FileChannel.read(buffers, 1, 2);
        assertEquals(10, bytesRead);
        assertEquals(0, buffers[0].position()); // Should not be touched
        assertEquals(5, buffers[1].limit());
        assertEquals(5, buffers[2].limit());
    }

    @Test
    public void testReadStartingAt() throws IOException {
        setupReadMocks();
        ByteBuffer dest = ByteBuffer.allocate(10);

        s3FileChannel.read(dest, 6L);

        // verify delegation by s3FileChannel
        verify(s3SeekableByteChannel).position(6L);
        verify(s3SeekableByteChannel).read(dest);
    }

    @Test
    public void testReadStartingAtNegativeNumber() throws IOException {
        ByteBuffer dest = ByteBuffer.allocate(10);
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
    
    @Test
    public void testTryLock() {
        assertThrowsExactly(
                IOException.class,
                () -> s3FileChannel.tryLock(),
                "S3 does not support file locks");
    }
    
    @Test
    public void testTryLockWithParameters() {
        assertThrowsExactly(
                IOException.class,
                () -> s3FileChannel.tryLock(0, 100, false),
                "S3 does not support file locks");
    }
    
    @Test
    public void testLockWithParameters() {
        assertThrowsExactly(
                IOException.class,
                () -> s3FileChannel.lock(0, 100, false),
                "S3 does not support file locks");
    }
    
    @Test
    public void testWrite() throws IOException {
        setupWriteMocks();
        ByteBuffer src = ByteBuffer.wrap("test data".getBytes());
        
        int bytesWritten = s3FileChannel.write(src);
        
        assertEquals(9, bytesWritten);
        verify(s3SeekableByteChannel).write(src);
    }
    
    @Test
    public void testWriteWithPosition() throws IOException {
        setupWriteMocks();
        ByteBuffer src = ByteBuffer.wrap("test data".getBytes());
        when(s3SeekableByteChannel.position()).thenReturn(0L);
        
        int bytesWritten = s3FileChannel.write(src, 10L);
        
        assertEquals(9, bytesWritten);
        verify(s3SeekableByteChannel).position(10L);
        verify(s3SeekableByteChannel).write(src);
        verify(s3SeekableByteChannel).position(0L);
    }
    
    @Test
    public void testWriteWithNegativePosition() throws IOException {
        ByteBuffer src = ByteBuffer.wrap("test data".getBytes());
        
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> s3FileChannel.write(src, -5L),
                "position is negative");
    }
    
    @Test
    public void testGatheringWrite() throws IOException {
        setupWriteMocks();
        ByteBuffer[] srcs = {
            ByteBuffer.wrap("test ".getBytes()),
            ByteBuffer.wrap("data".getBytes())
        };
        
        long bytesWritten = s3FileChannel.write(srcs, 0, srcs.length);
        
        assertEquals(9, bytesWritten);
        verify(s3SeekableByteChannel, times(2)).write(any(ByteBuffer.class));
    }
    
    @Test
    public void testGatheringWriteWithOffset() throws IOException {
        setupWriteMocks();
        ByteBuffer[] srcs = {
            ByteBuffer.wrap("ignore ".getBytes()),
            ByteBuffer.wrap("test ".getBytes()),
            ByteBuffer.wrap("data".getBytes())
        };
        
        long bytesWritten = s3FileChannel.write(srcs, 1, 2);
        
        assertEquals(9, bytesWritten);
        verify(s3SeekableByteChannel, times(2)).write(any(ByteBuffer.class));
    }
    
    @Test
    public void testTruncate() throws IOException {
        s3FileChannel.truncate(100L);
        verify(s3SeekableByteChannel).truncate(100L);
    }
    
    @Test
    public void testForce() throws IOException {
        when(s3SeekableByteChannel.getWriteDelegate()).thenReturn(writeDelegate);
        
        s3FileChannel.force(true);
        
        verify(writeDelegate).force();
    }
    
    @Test
    public void testForceWithNoWriteDelegate() throws IOException {
        when(s3SeekableByteChannel.getWriteDelegate()).thenReturn(null);
        
        // Should not throw exception
        s3FileChannel.force(true);
    }
    
    @Test
    public void testTransferTo() throws IOException {
        // Setup
        WritableByteChannel target = mock(WritableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(100L);
        when(s3SeekableByteChannel.getReadDelegate()).thenReturn(mock(S3ReadAheadByteChannel.class));
        when(s3SeekableByteChannel.isOpen()).thenReturn(true);
        doReturn(0L).when(s3SeekableByteChannel).position();
        setupReadMocks();
        when(target.write(any(ByteBuffer.class))).thenReturn(10);
        
        // Execute
        long transferred = s3FileChannel.transferTo(0, 10, target);
        
        // Verify
        assertEquals(10, transferred);
        verify(s3SeekableByteChannel, times(2)).position(0L);
        verify(target).write(any(ByteBuffer.class));
    }
    
    @Test
    public void testTransferToWithNegativePosition() throws IOException {
        WritableByteChannel target = mock(WritableByteChannel.class);
        
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> s3FileChannel.transferTo(-1, 10, target),
                "position must be non-negative");
    }
    
    @Test
    public void testTransferToWithNegativeCount() throws IOException {
        WritableByteChannel target = mock(WritableByteChannel.class);
        
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> s3FileChannel.transferTo(0, -10, target),
                "count must be non-negative");
    }
    
    @Test
    public void testTransferToWithZeroCount() throws IOException {
        WritableByteChannel target = mock(WritableByteChannel.class);
        
        long transferred = s3FileChannel.transferTo(0, 0, target);
        
        assertEquals(0, transferred);
        verifyNoMoreInteractions(target);
    }
    
    @Test
    public void testTransferToWithPositionBeyondSize() throws IOException {
        WritableByteChannel target = mock(WritableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(10L);
        
        long transferred = s3FileChannel.transferTo(20, 10, target);
        
        assertEquals(0, transferred);
        verifyNoMoreInteractions(target);
    }
    
    @Test
    public void testTransferToWithNoReadDelegate() throws IOException {
        WritableByteChannel target = mock(WritableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(100L);
        when(s3SeekableByteChannel.getReadDelegate()).thenReturn(null);
        
        assertThrowsExactly(
                NonReadableChannelException.class,
                () -> s3FileChannel.transferTo(0, 10, target));
    }
    
    @Test
    public void testTransferToWithClosedChannel() throws IOException {
        WritableByteChannel target = mock(WritableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(100L);
        when(s3SeekableByteChannel.getReadDelegate()).thenReturn(mock(S3ReadAheadByteChannel.class));
        when(s3SeekableByteChannel.isOpen()).thenReturn(false);
        
        assertThrowsExactly(
                ClosedChannelException.class,
                () -> s3FileChannel.transferTo(0, 10, target));
    }
    
    @Test
    public void testTransferFrom() throws IOException {
        // Setup
        ReadableByteChannel src = mock(ReadableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(100L);
        when(s3SeekableByteChannel.getWriteDelegate()).thenReturn(mock(S3WritableByteChannel.class));
        when(s3SeekableByteChannel.isOpen()).thenReturn(true);
        when(s3SeekableByteChannel.position()).thenReturn(0L).thenReturn(0L);
        setupWriteMocks();
        when(src.read(any(ByteBuffer.class))).thenReturn(10).thenReturn(-1);
        
        // Execute
        long transferred = s3FileChannel.transferFrom(src, 0, 20);
        
        // Verify
        assertEquals(10, transferred);
        verify(s3SeekableByteChannel, times(2)).position(0L);
        verify(src, times(2)).read(any(ByteBuffer.class));
        verify(s3SeekableByteChannel).write(any(ByteBuffer.class));
    }
    
    @Test
    public void testTransferFromWithNegativePosition() throws IOException {
        ReadableByteChannel src = mock(ReadableByteChannel.class);
        
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> s3FileChannel.transferFrom(src, -1, 10),
                "file position must be non-negative");
    }
    
    @Test
    public void testTransferFromWithNegativeCount() throws IOException {
        ReadableByteChannel src = mock(ReadableByteChannel.class);
        
        assertThrowsExactly(
                IllegalArgumentException.class,
                () -> s3FileChannel.transferFrom(src, 0, -10),
                "byte count must be non-negative");
    }
    
    @Test
    public void testTransferFromWithZeroCount() throws IOException {
        ReadableByteChannel src = mock(ReadableByteChannel.class);
        
        long transferred = s3FileChannel.transferFrom(src, 0, 0);
        
        assertEquals(0, transferred);
        verifyNoMoreInteractions(src);
    }
    
    @Test
    public void testTransferFromWithPositionBeyondSize() throws IOException {
        ReadableByteChannel src = mock(ReadableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(10L);
        
        long transferred = s3FileChannel.transferFrom(src, 20, 10);
        
        assertEquals(0, transferred);
        verifyNoMoreInteractions(src);
    }
    
    @Test
    public void testTransferFromWithNoWriteDelegate() throws IOException {
        ReadableByteChannel src = mock(ReadableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(100L);
        when(s3SeekableByteChannel.getWriteDelegate()).thenReturn(null);
        
        assertThrowsExactly(
                NonWritableChannelException.class,
                () -> s3FileChannel.transferFrom(src, 0, 10));
    }
    
    @Test
    public void testTransferFromWithClosedChannel() throws IOException {
        ReadableByteChannel src = mock(ReadableByteChannel.class);
        when(s3SeekableByteChannel.size()).thenReturn(100L);
        when(s3SeekableByteChannel.getWriteDelegate()).thenReturn(mock(S3WritableByteChannel.class));
        when(s3SeekableByteChannel.isOpen()).thenReturn(false);
        
        assertThrowsExactly(
                ClosedChannelException.class,
                () -> s3FileChannel.transferFrom(src, 0, 10));
    }
}
