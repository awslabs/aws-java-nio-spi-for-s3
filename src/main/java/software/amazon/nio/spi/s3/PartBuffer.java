/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.ByteBuffer;

/**
 * Internal helper that manages a byte buffer for accumulating part data during streaming multipart uploads.
 *
 * <p>
 * A {@code PartBuffer} wraps a {@link ByteBuffer} of configurable size (the part size) and provides methods to
 * write data into the buffer, check if it is full, and prepare it for reading (upload).
 */
class PartBuffer {

    private final ByteBuffer buffer;

    /**
     * Creates a new PartBuffer with the specified part size.
     *
     * @param partSize the size of the buffer in bytes
     * @throws IllegalArgumentException if partSize is not positive
     */
    PartBuffer(int partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("partSize must be positive, got: " + partSize);
        }
        this.buffer = ByteBuffer.allocate(partSize);
    }

    /**
     * Copies bytes from the source buffer into this part buffer.
     *
     * <p>
     * The number of bytes actually written may be less than {@code src.remaining()} if this buffer fills up.
     *
     * @param src the source buffer to copy bytes from
     * @return the number of bytes written into this buffer
     */
    int write(ByteBuffer src) {
        int bytesToWrite = Math.min(src.remaining(), buffer.remaining());
        if (bytesToWrite > 0) {
            // Use a limited view of src to avoid writing more than we can hold
            int srcLimit = src.limit();
            src.limit(src.position() + bytesToWrite);
            buffer.put(src);
            src.limit(srcLimit);
        }
        return bytesToWrite;
    }

    /**
     * Returns whether this buffer is full (no remaining capacity for writing).
     *
     * @return {@code true} if the buffer has no remaining capacity
     */
    boolean isFull() {
        return buffer.remaining() == 0;
    }

    /**
     * Prepares this buffer for reading by flipping the underlying {@link ByteBuffer}.
     *
     * <p>
     * After calling this method, the buffer's position is set to zero and its limit is set to the number of bytes
     * that were written. This prepares the buffer for uploading its content as a part.
     *
     * @return the underlying ByteBuffer, flipped and ready for reading
     */
    ByteBuffer flip() {
        buffer.flip();
        return buffer;
    }

    /**
     * Returns the number of bytes that can still be written into this buffer.
     *
     * @return the remaining capacity for writing
     */
    int remaining() {
        return buffer.remaining();
    }
}
