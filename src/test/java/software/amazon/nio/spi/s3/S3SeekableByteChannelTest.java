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
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.After;

import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class S3SeekableByteChannelTest {

    S3FileSystem fs;
    S3Path path;
    byte[] bytes = "abcdef".getBytes(StandardCharsets.UTF_8);

    @Mock
    S3AsyncClient mockClient;

    @Before
    public void init() {
        // forward to the method that uses the HeadObjectRequest parameter
        when(mockClient.headObject(any(Consumer.class))).thenCallRealMethod();
        when(mockClient.headObject(any(HeadObjectRequest.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
        when(mockClient.getObject(any(Consumer.class), any(AsyncResponseTransformer.class))).thenCallRealMethod();
        when(mockClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> ResponseBytes.fromByteArray(
                        GetObjectResponse.builder().contentLength(6L).build(),
                        bytes)));

        S3FileSystemProvider provider = new S3FileSystemProvider();
        provider.clientProvider = new S3ClientProvider() {
            @Override
            protected S3AsyncClient generateAsyncClient(String bucketName) {
                return mockClient;
            }
        };
        fs = provider.newFileSystem(URI.create("s3://test-bucket"), Collections.EMPTY_MAP);
        path = fs.getPath("/object");
    }

    @After
    public void after() throws IOException {
        fs.close();
    }

    @Test
    public void readDelegateConstructedByDefault() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        assertNotNull(channel.getReadDelegate());
    }

    @Test
    public void read() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        ByteBuffer dst = ByteBuffer.allocate(6);
        channel.read(dst);
        assertArrayEquals(bytes, dst.array());
        assertEquals(6L, channel.position());
    }

    @Test
    public void write() throws IOException {
        when(mockClient.headObject(any(HeadObjectRequest.class))).thenThrow(NoSuchKeyException.class);
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                PutObjectResponse.builder().build()));
        Set<OpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.CREATE);
        options.add(StandardOpenOption.WRITE);
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient, options);
        channel.write(ByteBuffer.allocate(1));
        channel.close();
    }

    @Test
    public void position() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        assertEquals(0L, channel.position());
    }

    @Test
    public void testPosition() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        channel.position(1L);
        assertEquals(1L, channel.position());
    }

    @Test
    public void size() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        assertEquals(100L, channel.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void truncate() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        channel.truncate(0L);
    }

    @Test
    public void isOpen() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        assertTrue(channel.isOpen());
    }

    @Test
    public void close() throws IOException {
        S3SeekableByteChannel channel = new S3SeekableByteChannel(path, mockClient);
        channel.close();
        assertFalse(channel.isOpen());
    }
}