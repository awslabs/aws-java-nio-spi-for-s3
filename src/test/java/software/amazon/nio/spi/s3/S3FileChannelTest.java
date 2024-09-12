/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
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

    private final byte[] testBytes = "ABCDEFGHIJ".getBytes();

    @BeforeEach
    public void init() throws IOException {
        s3FileChannel = new S3FileChannel(s3SeekableByteChannel);
        ByteBuffer s3FileChannelBuffer = ByteBuffer.wrap(testBytes);

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
        ByteBuffer dest  = ByteBuffer.allocate(10);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(bytesRead, 10);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
    public void testReadLessBytes() throws IOException {
        ByteBuffer dest  = ByteBuffer.allocate(5);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(bytesRead, 5);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
    public void testReadMoreBytes() throws IOException {
        ByteBuffer dest  = ByteBuffer.allocate(15);

        int bytesRead = s3FileChannel.read(dest);
        assertEquals(bytesRead, testBytes.length);
        for (int i = 0; i < bytesRead; i++) {
            assertEquals(dest.get(i), testBytes[i]);
        }
    }

    @Test
    public void testScatterRead() throws IOException {
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
}
