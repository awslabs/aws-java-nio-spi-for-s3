/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;
import software.amazon.nio.spi.s3.util.TimeOutUtils;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class S3SeekableByteChannel implements SeekableByteChannel {

    private long position;
    private final S3AsyncClient s3Client;
    private final S3Path path;
    private final ReadableByteChannel readDelegate;
    //private final WritableByteChannel writeDelegate; // placeholder for when we implement writes.

    private long size = -1L;

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected S3SeekableByteChannel(S3Path s3Path, S3AsyncClient s3Client, long startAt) throws IOException{
        position = startAt;
        path = s3Path;
        this.s3Client = s3Client;
        s3Path.getFileSystem().registerOpenChannel(this);

        // later we will add a constructor that allows providing delegates for composition
        logger.info("using S3ReadAheadByteChannel as read delegate for path '{}'", s3Path.toUri());
        S3NioSpiConfiguration config = new S3NioSpiConfiguration();
        readDelegate = new S3ReadAheadByteChannel(s3Path, config.getMaxFragmentSize(), config.getMaxFragmentNumber(), s3Client, this);
    }

    /**
     * Open a new byte channel
     * @param s3Path the path that the channel will read from
     * @param startAt the byte offset to start at (0 is the beginning).
     */
    protected S3SeekableByteChannel(S3Path s3Path, long startAt) throws IOException {
        this(s3Path, S3ClientStore.getInstance().getAsyncClientForBucketName(s3Path.bucketName()), startAt);
    }

    /**
     * Equivalent to: {@code new S3SeekableByteChannel(s3Path, 0L)}
     * @param s3Path the path to open the byte channel for.
     */
    protected S3SeekableByteChannel(S3Path s3Path) throws IOException {
        this(s3Path, S3ClientStore.getInstance().getAsyncClientForBucketName(s3Path.bucketName()), 0L);
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p> Bytes are read starting at this channel's current position, and
     * then the position is updated with the number of bytes actually read.
     * Otherwise, this method behaves exactly as specified in the {@link
     * ReadableByteChannel} interface.
     *
     * @param dst the destination buffer
     * @return the number of bytes read or -1 if no more bytes can be read.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return readDelegate.read(dst);
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p> Bytes are written starting at this channel's current position, unless
     * the channel is connected to an entity such as a file that is opened with
     * the {@link StandardOpenOption#APPEND APPEND} option, in
     * which case the position is first advanced to the end. The entity to which
     * the channel is connected is grown, if necessary, to accommodate the
     * written bytes, and then the position is updated with the number of bytes
     * actually written. Otherwise, this method behaves exactly as specified by
     * the {@link WritableByteChannel} interface.
     *
     * @param src the src of the bytes to write to this channel
     */
    @Override
    public int write(ByteBuffer src) {
        throw new UnsupportedOperationException("this channel is read only");
    }

    /**
     * Returns this channel's position.
     *
     * @return This channel's position,
     * a non-negative integer counting the number of bytes
     * from the beginning of the entity to the current position
     */
    @Override
    public long position() {
        synchronized (this) {
            return position;
        }
    }

    /**
     * Sets this channel's position.
     *
     * <p> Setting the position to a value that is greater than the current size
     * is legal but does not change the size of the entity.  A later attempt to
     * read bytes at such a position will immediately return an end-of-file
     * indication.  A later attempt to write bytes at such a position will cause
     * the entity to grow to accommodate the new bytes; the values of any bytes
     * between the previous end-of-file and the newly-written bytes are
     * unspecified.
     *
     * <p> Setting the channel's position is not recommended when connected to
     * an entity, typically a file, that is opened with the {@link
     * StandardOpenOption#APPEND APPEND} option. When opened for
     * append, the position is first advanced to the end before writing.
     *
     * @param newPosition The new position, a non-negative integer counting
     *                    the number of bytes from the beginning of the entity
     * @return This channel
     * @throws ClosedChannelException   If this channel is closed
     * @throws IllegalArgumentException If the new position is negative
     * @throws IOException              If some other I/O error occurs
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0) throw new IllegalArgumentException("newPosition cannot be < 0");
        if (!isOpen()) {
            throw new ClosedChannelException();
        }

        synchronized (this) {
            position = newPosition;
            return this;
        }
    }

    /**
     * Returns the current size of entity to which this channel is connected.
     *
     * @return The current size, measured in bytes
     * @throws IOException            If some other I/O error occurs
     */
    @Override
    public long size() throws IOException {
        if (size < 0) {

            long timeOut = TimeOutUtils.TIMEOUT_TIME_LENGTH_1;
            TimeUnit unit = TimeUnit.MINUTES;

            logger.debug("requesting size of '{}'", path.toUri());
            synchronized (this) {
                final HeadObjectResponse headObjectResponse;
                try {
                    headObjectResponse = s3Client.headObject(builder -> builder
                            .bucket(path.bucketName())
                            .key(path.getKey())).get(timeOut, unit);
                } catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new IOException(e);
                } catch (TimeoutException e){
                    throw TimeOutUtils.logAndGenerateExceptionOnTimeOut(logger, "size", timeOut, unit);
                }

                logger.debug("size of '{}' is '{}'", path.toUri(), headObjectResponse.contentLength());
                this.size = headObjectResponse.contentLength();
            }
        }
        return this.size;
    }

    /**
     * Truncates the entity, to which this channel is connected, to the given
     * size.
     *
     * <p> If the given size is less than the current size then the entity is
     * truncated, discarding any bytes beyond the new end. If the given size is
     * greater than or equal to the current size then the entity is not modified.
     * In either case, if the current position is greater than the given size
     * then it is set to that size.
     *
     * <p> An implementation of this interface may prohibit truncation when
     * connected to an entity, typically a file, opened with the {@link
     * StandardOpenOption#APPEND APPEND} option.
     *
     * @param size The new size, a non-negative byte count
     * @return This channel
     */
    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException("this channel is read only");
    }

    /**
     * Tells whether this channel is open.
     *
     * @return <tt>true</tt> if, and only if, this channels delegate is open
     */
    @Override
    public boolean isOpen() {
        logger.debug("read delegate is: '{}'", readDelegate);
        synchronized (this) {
            return readDelegate.isOpen();
        }
    }

    /**
     * Closes this channel.
     *
     * <p> After a channel is closed, any further attempt to invoke I/O
     * operations upon it will cause a {@link ClosedChannelException} to be
     * thrown.
     *
     * <p> If this channel is already closed then invoking this method has no
     * effect.
     *
     * <p> This method may be invoked at any time.  If some other thread has
     * already invoked it, however, then another invocation will block until
     * the first invocation is complete, after which it will return without
     * effect. </p>
     *
     */
    @Override
    public void close() throws IOException {
        synchronized (this){
            readDelegate.close();
            path.getFileSystem().deregisterClosedChannel(this);
        }
    }
}
