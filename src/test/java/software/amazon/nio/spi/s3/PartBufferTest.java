/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class PartBufferTest {

    @Test
    void constructor_positivePartSize_createsBuffer() {
        PartBuffer buffer = new PartBuffer(1024);

        assertThat(buffer.remaining()).isEqualTo(1024);
        assertThat(buffer.isFull()).isFalse();
    }

    @Test
    void constructor_zeroPartSize_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> new PartBuffer(0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("partSize must be positive");
    }

    @Test
    void constructor_negativePartSize_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> new PartBuffer(-1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("partSize must be positive");
    }

    @Test
    void write_fillsBuffer_returnsCorrectBytesWritten() {
        PartBuffer buffer = new PartBuffer(10);
        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});

        int written = buffer.write(src);

        assertThat(written).isEqualTo(5);
        assertThat(buffer.remaining()).isEqualTo(5);
        assertThat(src.remaining()).isZero();
    }

    @Test
    void write_multipleWrites_accumulatesBytes() {
        PartBuffer buffer = new PartBuffer(10);
        ByteBuffer src1 = ByteBuffer.wrap(new byte[]{1, 2, 3});
        ByteBuffer src2 = ByteBuffer.wrap(new byte[]{4, 5, 6, 7});

        int written1 = buffer.write(src1);
        int written2 = buffer.write(src2);

        assertThat(written1).isEqualTo(3);
        assertThat(written2).isEqualTo(4);
        assertThat(buffer.remaining()).isEqualTo(3);
    }

    @Test
    void write_exactlyFillsBuffer_returnsFullSize() {
        PartBuffer buffer = new PartBuffer(5);
        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});

        int written = buffer.write(src);

        assertThat(written).isEqualTo(5);
        assertThat(buffer.remaining()).isZero();
        assertThat(buffer.isFull()).isTrue();
    }

    @Test
    void write_sourceExceedsCapacity_writesOnlyWhatFits() {
        PartBuffer buffer = new PartBuffer(5);
        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});

        int written = buffer.write(src);

        assertThat(written).isEqualTo(5);
        assertThat(src.remaining()).isEqualTo(3);
        assertThat(buffer.isFull()).isTrue();
    }

    @Test
    void write_emptySource_returnsZero() {
        PartBuffer buffer = new PartBuffer(10);
        ByteBuffer src = ByteBuffer.allocate(0);

        int written = buffer.write(src);

        assertThat(written).isZero();
        assertThat(buffer.remaining()).isEqualTo(10);
    }

    @Test
    void write_bufferAlreadyFull_returnsZero() {
        PartBuffer buffer = new PartBuffer(3);
        ByteBuffer src1 = ByteBuffer.wrap(new byte[]{1, 2, 3});
        buffer.write(src1);

        ByteBuffer src2 = ByteBuffer.wrap(new byte[]{4, 5});
        int written = buffer.write(src2);

        assertThat(written).isZero();
        assertThat(src2.remaining()).isEqualTo(2);
    }

    @Test
    void isFull_emptyBuffer_returnsFalse() {
        PartBuffer buffer = new PartBuffer(10);

        assertThat(buffer.isFull()).isFalse();
    }

    @Test
    void isFull_partiallyFilledBuffer_returnsFalse() {
        PartBuffer buffer = new PartBuffer(10);
        buffer.write(ByteBuffer.wrap(new byte[]{1, 2, 3}));

        assertThat(buffer.isFull()).isFalse();
    }

    @Test
    void isFull_completelyFilledBuffer_returnsTrue() {
        PartBuffer buffer = new PartBuffer(5);
        buffer.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));

        assertThat(buffer.isFull()).isTrue();
    }

    @Test
    void flip_preparesBufferForReading() {
        PartBuffer buffer = new PartBuffer(10);
        buffer.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));

        ByteBuffer flipped = buffer.flip();

        assertThat(flipped.position()).isZero();
        assertThat(flipped.limit()).isEqualTo(5);
        assertThat(flipped.remaining()).isEqualTo(5);
    }

    @Test
    void flip_fullBuffer_preparesEntireBufferForReading() {
        PartBuffer buffer = new PartBuffer(5);
        buffer.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}));

        ByteBuffer flipped = buffer.flip();

        assertThat(flipped.position()).isZero();
        assertThat(flipped.limit()).isEqualTo(5);
        assertThat(flipped.remaining()).isEqualTo(5);
    }

    @Test
    void flip_emptyBuffer_returnsEmptyReadableBuffer() {
        PartBuffer buffer = new PartBuffer(10);

        ByteBuffer flipped = buffer.flip();

        assertThat(flipped.position()).isZero();
        assertThat(flipped.limit()).isZero();
        assertThat(flipped.remaining()).isZero();
    }

    @Test
    void flip_contentIsCorrect() {
        PartBuffer buffer = new PartBuffer(10);
        byte[] data = {10, 20, 30, 40, 50};
        buffer.write(ByteBuffer.wrap(data));

        ByteBuffer flipped = buffer.flip();

        byte[] result = new byte[flipped.remaining()];
        flipped.get(result);
        assertThat(result).isEqualTo(data);
    }

    @Test
    void remaining_newBuffer_equalsPartSize() {
        PartBuffer buffer = new PartBuffer(256);

        assertThat(buffer.remaining()).isEqualTo(256);
    }

    @Test
    void remaining_afterWrite_decreasesByBytesWritten() {
        PartBuffer buffer = new PartBuffer(100);
        buffer.write(ByteBuffer.wrap(new byte[30]));

        assertThat(buffer.remaining()).isEqualTo(70);
    }

    @Test
    void remaining_fullBuffer_returnsZero() {
        PartBuffer buffer = new PartBuffer(5);
        buffer.write(ByteBuffer.wrap(new byte[5]));

        assertThat(buffer.remaining()).isZero();
    }

    @Test
    void write_boundaryWrite_sourcePositionAdvancesCorrectly() {
        PartBuffer buffer = new PartBuffer(5);
        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        int written = buffer.write(src);

        assertThat(written).isEqualTo(5);
        assertThat(src.position()).isEqualTo(5);
        assertThat(src.remaining()).isEqualTo(5);

        // The remaining bytes in src should be 6, 7, 8, 9, 10
        byte[] remaining = new byte[src.remaining()];
        src.get(remaining);
        assertThat(remaining).isEqualTo(new byte[]{6, 7, 8, 9, 10});
    }

    @Test
    void write_sourceWithOffset_writesFromCurrentPosition() {
        PartBuffer buffer = new PartBuffer(10);
        ByteBuffer src = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        src.position(3); // Skip first 3 bytes

        int written = buffer.write(src);

        assertThat(written).isEqualTo(5);
        assertThat(src.remaining()).isZero();

        ByteBuffer flipped = buffer.flip();
        byte[] result = new byte[flipped.remaining()];
        flipped.get(result);
        assertThat(result).isEqualTo(new byte[]{4, 5, 6, 7, 8});
    }
}
