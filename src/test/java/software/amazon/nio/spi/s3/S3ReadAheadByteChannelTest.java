/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
public class S3ReadAheadByteChannelTest {

    final S3FileSystemProvider provider = new S3FileSystemProvider();

    S3Path path;

    @Mock
    S3SeekableByteChannel delegator;

    @Mock
    S3AsyncClient client;

    S3ReadAheadByteChannel readAheadByteChannel;


    @BeforeEach
    public void setup() throws IOException {
        path = S3Path.getPath((S3FileSystem) provider.getFileSystem(URI.create("s3://my-bucket")), "/object");

        // mocking
        lenient().when(delegator.size()).thenReturn(52L);

        final var response = GetObjectResponse.builder().build();
        final var bytes1 = ResponseBytes.fromByteArray(response, "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8));
        final var bytes2 = ResponseBytes.fromByteArray(response, "ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(StandardCharsets.UTF_8));
        lenient().when(client.getObject(anyConsumer(), any(ByteArrayAsyncResponseTransformer.class))).thenReturn(CompletableFuture.supplyAsync(() -> bytes1), CompletableFuture.supplyAsync(() -> bytes2));

        readAheadByteChannel = new S3ReadAheadByteChannel(path, 26, 2, client, delegator, null, null);
    }

    @Test
    public void read6BytesFromPosition0() throws IOException {
        when(delegator.position()).thenReturn(0L);
        var dst = ByteBuffer.allocate(6);
        readAheadByteChannel.read(dst);
        assertArrayEquals("abcdef".getBytes(StandardCharsets.UTF_8), dst.array());
        assertEquals(1, readAheadByteChannel.numberOfCachedFragments());
    }

    @Test
    public void read26BytesFromPosition0() throws IOException {
        when(delegator.position()).thenReturn(0L);
        var dst = ByteBuffer.allocate(30);
        final var numBytesRead = readAheadByteChannel.read(dst);
        assertEquals(26, numBytesRead);
        // this should have triggered loading of the next fragment to the cache
        assertEquals(2, readAheadByteChannel.numberOfCachedFragments());
        assertEquals('a', dst.get(0));
        assertEquals('z', dst.get(25));
    }

    @Test
    public void read6BytesFromPosition1() throws IOException {
        when(delegator.position()).thenReturn(1L);
        var dst = ByteBuffer.allocate(6);
        readAheadByteChannel.read(dst);
        assertArrayEquals("bcdefg".getBytes(StandardCharsets.UTF_8), dst.array());
        assertEquals(1, readAheadByteChannel.numberOfCachedFragments());
    }

    @Test
    public void read25BytesFromPosition1() throws IOException {
        when(delegator.position()).thenReturn(1L);
        var dst = ByteBuffer.allocate(30);
        final var numBytesRead = readAheadByteChannel.read(dst);
        assertEquals(25, numBytesRead);
        //should have triggered loading of the next fragment to the cache
        assertEquals(2, readAheadByteChannel.numberOfCachedFragments());
        assertEquals('b', dst.get(0));
        assertEquals('z', dst.get(24));
    }

    @Test
    public void shouldBeTwoCachedFragments() throws IOException {
        when(delegator.position()).thenReturn(0L, 26L);
        var dst = ByteBuffer.allocate(6);
        readAheadByteChannel.read(dst);
        assertEquals(1, readAheadByteChannel.numberOfCachedFragments());

        dst = ByteBuffer.allocate(6);
        readAheadByteChannel.read(dst);
        assertEquals(2, readAheadByteChannel.numberOfCachedFragments());
        assertArrayEquals("ABCDEF".getBytes(StandardCharsets.UTF_8), dst.array());
    }

    @Test
    public void shouldBeACacheHit() throws IOException {
        when(delegator.position()).thenReturn(0L, 6L);
        var dst = ByteBuffer.allocate(6);
        readAheadByteChannel.read(dst);
        assertEquals(1, readAheadByteChannel.numberOfCachedFragments());

        dst = ByteBuffer.allocate(6);
        readAheadByteChannel.read(dst);
        assertEquals(1, readAheadByteChannel.numberOfCachedFragments());
        assertEquals(1, readAheadByteChannel.cacheStatistics().hitCount());
    }

    @Test
    public void shouldSignalFinished() throws IOException {
        when(delegator.position()).thenReturn(52L);
        var dst = ByteBuffer.allocate(6);

        assertEquals(-1, readAheadByteChannel.read(dst));
    }


    @Test
    public void isOpen() {
        assertTrue(readAheadByteChannel.isOpen());
    }

    @Test
    public void close() throws IOException {
        readAheadByteChannel.read(ByteBuffer.allocate(10));
        assertTrue(readAheadByteChannel.numberOfCachedFragments() > 0);

        readAheadByteChannel.close();
        assertFalse(readAheadByteChannel.isOpen());
        assertEquals(0, readAheadByteChannel.numberOfCachedFragments());
    }

    @Test
    public void fragmentIndexForByteNumber() {
        assertEquals(Integer.valueOf(0), readAheadByteChannel.fragmentIndexForByteNumber(0L));
        assertEquals(Integer.valueOf(1), readAheadByteChannel.fragmentIndexForByteNumber(26L));
    }
}