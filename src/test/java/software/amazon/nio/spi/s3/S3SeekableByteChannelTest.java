/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
public class S3SeekableByteChannelTest {

    S3FileSystem fs;
    S3Path path;
    byte[] bytes = "abcdef".getBytes(StandardCharsets.UTF_8);

    @Mock
    S3AsyncClient mockClient;

    @BeforeEach
    public void init() {
        // forward to the method that uses the HeadObjectRequest parameter
        lenient().when(mockClient.headObject(anyConsumer())).thenCallRealMethod();
        lenient().when(mockClient.headObject(any(HeadObjectRequest.class))).thenReturn(
                CompletableFuture.completedFuture(
                    HeadObjectResponse.builder().contentLength(100L).lastModified(Instant.now()).build()
                )
        );
        lenient().when(mockClient.getObject(anyConsumer(), any(AsyncResponseTransformer.class))).thenCallRealMethod();
        lenient().when(mockClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> ResponseBytes.fromByteArray(
                        GetObjectResponse.builder().contentLength(6L).build(),
                        bytes)));

        var provider = new S3FileSystemProvider();
        fs = provider.getFileSystem(URI.create("s3://test-bucket"), true);
        fs.clientProvider(new FixedS3ClientProvider(mockClient));
        path = (S3Path) fs.getPath("/object");
    }

    @AfterEach
    public void after() throws IOException {
        fs.close();
    }

    @Test
    public void readDelegateConstructedByDefault() throws IOException {
        try(var channel = seekableByteChannelForRead()) {
            assertNotNull(channel.getReadDelegate());
        }
    }

    @Test
    public void read() throws IOException {
        try(var channel = seekableByteChannelForRead()) {
            var dst = ByteBuffer.allocate(6);
            channel.read(dst);
            assertArrayEquals(bytes, dst.array());
            assertEquals(6L, channel.position());
        }
    }

    @Test
    public void write() throws IOException {
        when(mockClient.headObject(any(HeadObjectRequest.class))).thenThrow(NoSuchKeyException.class);
        when(mockClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(CompletableFuture.supplyAsync(() ->
                PutObjectResponse.builder().build()));
        try(var channel = new S3SeekableByteChannel(path, mockClient, Set.<OpenOption>of(CREATE, WRITE))){
            channel.write(ByteBuffer.allocate(1));
        }
    }

    @Test
    public void position() throws IOException {
        try(var channel = seekableByteChannelForRead()) {
            assertEquals(0L, channel.position());
        }
    }

    @Test
    public void testPosition() throws IOException {
        try(var channel = seekableByteChannelForRead()) {
            channel.position(1L);
            assertEquals(1L, channel.position());
        }
    }

    @Test
    public void size() throws IOException {
        try(var channel = seekableByteChannelForRead()) {
            assertEquals(100L, channel.size());
        }
    }

    @Test
    public void truncate() throws IOException {
        try(var channel = seekableByteChannelForRead()) {
            assertThrows(UnsupportedOperationException.class, () -> channel.truncate(0L));
        }
    }

    @Test
    public void isOpen() throws IOException {
        try(var channel = seekableByteChannelForRead()) {
            assertTrue(channel.isOpen());
        }
    }

    @Test
    public void close() throws IOException {
        var channel = seekableByteChannelForRead();
        channel.close();
        assertFalse(channel.isOpen());
    }

    private S3SeekableByteChannel seekableByteChannelForRead() throws IOException {
        return new S3SeekableByteChannel(path, mockClient, Collections.singleton(READ));
    }

    // test that the S3SeekableByteChannel uses the buffer size from the configuration set for the FileSystem
    @Test
    public void testBufferSize() throws IOException {
        fs.configuration().withMaxFragmentSize(10000);
        fs.configuration().withMaxFragmentNumber(10);
        try(var channel = (S3SeekableByteChannel) fs.provider().newByteChannel(path, Set.of(READ))) {
            assertEquals(10000, ((S3ReadAheadByteChannel) channel.getReadDelegate()).getMaxFragmentSize());
            assertEquals(10, ((S3ReadAheadByteChannel) channel.getReadDelegate()).getMaxNumberFragments());
        }
    }

}
