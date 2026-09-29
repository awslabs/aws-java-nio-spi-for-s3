/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Regression tests for <a href="https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/761">#761</a>.
 *
 * <p>Reads that span more than one read-ahead fragment must fill the destination buffer instead of
 * returning a short read at the fragment boundary. Callers such as htsjdk's
 * {@code IndexedFastaSequenceFile} issue a single {@code channel.read(buffer)} and assume the buffer
 * is filled the way a local {@code FileChannel} would be; a short read shifts their parsing and can
 * leak a line-terminator byte (10, {@code '\n'}) into the returned bases.
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
public class S3ReadAheadByteChannelFragmentBoundaryTest {

    final S3FileSystemProvider provider = new S3FileSystemProvider();

    S3Path path;

    @Mock
    S3SeekableByteChannel delegator;

    @Mock
    S3AsyncClient client;

    byte[] backing;
    final long[] positionHolder = {0L};

    private void initChannel(byte[] data, int fragmentSize, int maxFragments) throws IOException {
        this.backing = data;
        path = S3Path.getPath((S3FileSystem) provider.getFileSystem(URI.create("s3://my-bucket")), "/object");

        lenient().when(delegator.size()).thenReturn((long) backing.length);
        lenient().when(delegator.position()).thenAnswer(i -> positionHolder[0]);
        lenient().when(delegator.position(anyLong())).thenAnswer(i -> {
            positionHolder[0] = i.getArgument(0);
            return delegator;
        });

        // Return exactly the requested byte range, honouring the Range header like real S3.
        lenient().when(client.getObject(anyConsumer(), any(ByteArrayAsyncResponseTransformer.class)))
            .thenAnswer(invocation -> {
                Consumer<GetObjectRequest.Builder> consumer = invocation.getArgument(0);
                var builder = GetObjectRequest.builder();
                consumer.accept(builder);
                var spec = builder.build().range().substring("bytes=".length());
                var parts = spec.split("-");
                int from = Integer.parseInt(parts[0]);
                int to = Integer.parseInt(parts[1]);
                int len = to - from + 1;
                var slice = new byte[len];
                System.arraycopy(backing, from, slice, 0, len);
                return CompletableFuture.completedFuture(
                    ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), slice));
            });

        // Use the construction path with explicit fragment sizing.
        this.readAheadByteChannel =
            new S3ReadAheadByteChannel(path, fragmentSize, maxFragments, client, delegator, null, null);
    }

    S3ReadAheadByteChannel readAheadByteChannel;

    @BeforeEach
    public void resetPosition() {
        positionHolder[0] = 0L;
    }

    @Test
    public void singleReadFillsBufferAcrossFragmentBoundary() throws IOException {
        var data = new byte[100];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ('A' + (i % 26));
        }
        initChannel(data, 10, 50); // 10-byte fragments

        positionHolder[0] = 5L;            // start inside fragment 0 (bytes 0..9)
        var dst = ByteBuffer.allocate(20); // spans fragments 0, 1 and into 2

        int n = readAheadByteChannel.read(dst);

        assertEquals(20, n, "read() must fill the buffer across fragment boundaries");
        var expected = new byte[20];
        System.arraycopy(data, 5, expected, 0, 20);
        assertArrayEquals(expected, dst.array());
    }

    @Test
    public void readStopsAtEndOfObjectNotJustFragment() throws IOException {
        var data = new byte[25];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        initChannel(data, 10, 50); // fragments: [0..9][10..19][20..24]

        positionHolder[0] = 8L;
        var dst = ByteBuffer.allocate(1000); // ask for far more than remains

        int n = readAheadByteChannel.read(dst);

        // Should return all remaining bytes (8..24 = 17 bytes), spanning all three fragments.
        assertEquals(17, n);
        var expected = new byte[17];
        System.arraycopy(data, 8, expected, 0, 17);
        var got = new byte[17];
        dst.flip();
        dst.get(got);
        assertArrayEquals(expected, got);

        // A subsequent read at EOF returns -1.
        positionHolder[0] = 25L;
        assertEquals(-1, readAheadByteChannel.read(ByteBuffer.allocate(8)));
    }

    /**
     * Faithful port of htsjdk's {@code IndexedFastaSequenceFile} access pattern: it seeks to an
     * absolute position, issues a single {@code read(buffer)}, then walks the buffer skipping
     * line terminators at fixed intervals. This reproduces the original bug: before the fix, a
     * read straddling a fragment boundary returned short, the walker misaligned, and a {@code '\n'}
     * leaked into the bases. The reference sequence is verified byte-for-byte.
     */
    @Test
    public void htsjdkStyleNewlineWalkerAcrossFragmentBoundary() throws IOException {
        final int basesPerLine = 6;
        final int bytesPerLine = basesPerLine + 1; // + '\n'
        final int numLines = 40;

        // Build a FASTA-body-like buffer: lines of bases terminated by '\n'.
        var body = new StringBuilder();
        final byte[] alphabet = "ACGT".getBytes();
        int baseCounter = 0;
        for (int line = 0; line < numLines; line++) {
            for (int b = 0; b < basesPerLine; b++) {
                body.append((char) alphabet[baseCounter % alphabet.length]);
                baseCounter++;
            }
            body.append('\n');
        }
        var data = body.toString().getBytes();

        // Choose a fragment size that is NOT a multiple of bytesPerLine so reads straddle
        // boundaries at varied offsets relative to the line structure.
        initChannel(data, 13, 50);

        final int totalBases = numLines * basesPerLine;

        // The expected contiguous base sequence (no terminators).
        var expectedBases = new byte[totalBases];
        for (int i = 0; i < totalBases; i++) {
            expectedBases[i] = alphabet[i % alphabet.length];
        }

        // Query many sub-ranges, including ones that start/stop at varied positions, to force reads
        // that straddle fragment boundaries.
        for (int start = 0; start < totalBases; start += 1) {
            int stop = Math.min(totalBases, start + 17);
            var got = readBasesHtsjdkStyle(start, stop, basesPerLine, bytesPerLine);
            var expected = new byte[stop - start];
            System.arraycopy(expectedBases, start, expected, 0, stop - start);
            assertArrayEquals(expected, got, "base mismatch for [" + start + "," + stop + ")");
        }
    }

    /**
     * Minimal re-implementation of htsjdk's subsequence read loop against our channel.
     *
     * @param start 0-based inclusive start base index
     * @param stop  0-based exclusive stop base index
     */
    private byte[] readBasesHtsjdkStyle(int start, int stop, int basesPerLine, int bytesPerLine)
            throws IOException {
        final int terminatorLength = bytesPerLine - basesPerLine;
        final int length = stop - start;
        final byte[] target = new byte[length];
        final ByteBuffer targetBuffer = ByteBuffer.wrap(target);

        long startOffset = ((long) (start) / basesPerLine) * bytesPerLine + (start) % basesPerLine;
        final ByteBuffer channelBuffer = ByteBuffer.allocate(Math.max(bytesPerLine * 2, 16));

        while (targetBuffer.position() < length) {
            startOffset += Math.max((int) (startOffset % bytesPerLine - basesPerLine + 1), 0);

            startOffset += readFromPosition(channelBuffer, startOffset);
            channelBuffer.flip();

            final int positionInContig = start + targetBuffer.position();
            final int nextBaseSpan =
                Math.min(basesPerLine - positionInContig % basesPerLine, length - targetBuffer.position());
            int bytesToTransfer = Math.min(nextBaseSpan, channelBuffer.capacity());
            channelBuffer.limit(channelBuffer.position() + bytesToTransfer);

            while (channelBuffer.hasRemaining()) {
                targetBuffer.put(channelBuffer);
                bytesToTransfer = Math.min(basesPerLine, length - targetBuffer.position());
                channelBuffer.limit(Math.min(
                    channelBuffer.position() + bytesToTransfer + terminatorLength, channelBuffer.capacity()));
                channelBuffer.position(Math.min(channelBuffer.position() + terminatorLength, channelBuffer.capacity()));
            }
            channelBuffer.flip();
        }
        return target;
    }

    /** Mirrors htsjdk's non-FileChannel readFromPosition: save position, seek, read, restore. */
    private int readFromPosition(ByteBuffer buffer, long position) throws IOException {
        long oldPos = positionHolder[0];
        try {
            positionHolder[0] = position;
            return readAheadByteChannel.read(buffer);
        } finally {
            positionHolder[0] = oldPos;
        }
    }
}
