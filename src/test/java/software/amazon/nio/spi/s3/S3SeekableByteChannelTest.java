/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class S3SeekableByteChannelTest {

    S3FileSystem fs;
    S3SeekableByteChannel channel;
    S3Path path;
    byte[] bytes = "abcdef".getBytes(StandardCharsets.UTF_8);

    @Mock
    S3AsyncClient mockClient;

    @Before
    public void init() throws IOException {

        when(mockClient.headObject(any(Consumer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
        when(mockClient.getObject(any(Consumer.class), any(AsyncResponseTransformer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> ResponseBytes.fromByteArray(
                        GetObjectResponse.builder().contentLength(6L).build(),
                        bytes)));

        fs = new S3FileSystem("test-bucket");
        path = fs.getPath("/object");
        channel = new S3SeekableByteChannel(path, mockClient, 0L);
    }

    @Test
    public void read() throws IOException{
        ByteBuffer dst = ByteBuffer.allocate(6);
        channel.read(dst);
        assertArrayEquals(bytes, dst.array());
        assertEquals(6L, channel.position());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void write() {
        channel.write(ByteBuffer.allocate(1));
    }

    @Test
    public void position() {
        assertEquals(0L, channel.position());
    }

    @Test
    public void testPosition() throws IOException {
        channel.position(1L);
        assertEquals(1L, channel.position());
    }

    @Test
    public void size() throws IOException {
        assertEquals(100L, channel.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void truncate() {
        channel.truncate(0L);
    }

    @Test
    public void isOpen() {
        assertTrue(channel.isOpen());
    }

    @Test
    public void close() throws IOException{
        channel.close();
        assertFalse(channel.isOpen());
    }
}