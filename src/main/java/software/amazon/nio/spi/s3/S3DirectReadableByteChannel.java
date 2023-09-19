/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

/**
 * A delegate to provide byte reading from S3 to the delegating {@code S3SeekableByteChannel}. As reads are made
 * on behalf of the {@code delegator} this class will update the delegators {@code position} appropriately.
 */
public class S3DirectReadableByteChannel implements ReadableByteChannel {

    private final S3Client client;
    private final S3Path path;
    private final S3SeekableByteChannel delegator;


    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public S3DirectReadableByteChannel(S3Path path, S3Client client, S3SeekableByteChannel delegator) {
        this.path = path;
        this.client = client;
        this.delegator = delegator;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        String key = path.getKey();
        Objects.requireNonNull(dst, "The destination byte buffer of a read operation may not be null");
        synchronized (this) {
            logger.debug("channel position: {} of {}", delegator.position(), key);

            long size = delegator.size();
            logger.debug("{} size: {}", key, size);

            if (delegator.position() >= size) {
                logger.debug("completed reading from {}", key);
                return -1;
            }

            /*
             * Suppose that a byte sequence of length n is read, where
             * 0 <= n <= r.
             * This byte sequence will be transferred into the buffer of limit r so that the first
             * byte in the sequence is at index p and the last byte is at index
             * p + n - 1, where p is the buffer's position at the moment this method is
             * invoked.  Upon return the buffer's position will be equal to
             * p + n. Its limit will not have changed.
             */

            logger.debug("buffer position = {}, buffer limit = {}, buffer remaining: {}", dst.position(), dst.limit(), dst.remaining());

            long readTo = Math.min(dst.limit() - dst.position(), size - delegator.position()) - 1;

            String range = "bytes=" + delegator.position() + "-" + (delegator.position() + readTo);
            logger.debug("byte range for {} is '{}'", key, range);

            final ResponseBytes<GetObjectResponse> responseBytes = client.getObjectAsBytes(builder -> builder
                    .bucket(path.bucketName())
                    .key(path.getKey())
                    .range(range));

            byte[] s3Bytes = responseBytes.asByteArray();
            logger.debug("read {} bytes from {}", s3Bytes.length, key);

            delegator.position(delegator.position() + s3Bytes.length);
            logger.debug("new position of {} is: {}", key, delegator.position());

            int amountToWrite = Math.min(s3Bytes.length, dst.remaining());
            logger.debug("amount to write to buffer: {}, buffer remaining: {}", amountToWrite, dst.remaining());

            dst.put(s3Bytes, 0, amountToWrite);
            logger.debug("new buffer position is {}", dst.position());

            return s3Bytes.length;
        }
    }

    /**
     * This channel is always open
     */
    @Override
    public boolean isOpen() {
        return true;
    }

    /**
     * No op, this channel doesn't close as there are no resources to release.
     */
    @Override
    public void close() {}
}
