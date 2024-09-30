/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.FileLockInterruptionException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;

public class S3FileChannel extends FileChannel {

    private final S3SeekableByteChannel byteChannel;

    S3FileChannel(S3SeekableByteChannel byteChannel) {
        this.byteChannel = byteChannel;
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p> Bytes are read starting at this channel's current file position, and
     * then the file position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the {@link
     * ReadableByteChannel} interface. </p>
     *
     * @param dst the destination to read bytes into.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return byteChannel.read(dst);
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the
     * given buffers.
     *
     * <p> Bytes are read starting at this channel's current file position, and
     * then the file position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the {@link
     * ScatteringByteChannel} interface.  </p>
     *
     * @param dsts the buffer array
     * @param offset the index of the destination buffer in the buffer array to start reading into
     * @param length the number of buffers in the array to read bytes into
     * @return the number of bytes read into the buffer
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        int totalBytesRead = 0;
        for (int i = offset; i < offset + length; i++) {
            ByteBuffer dst = dsts[i];
            int bytesRead = read(dst);
            if (bytesRead == -1) {
                break;
            }
            totalBytesRead += bytesRead;
        }
        return totalBytesRead;
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p> Bytes are written starting at this channel's current file position
     * unless the channel is in append mode, in which case the position is
     * first advanced to the end of the file.  The file is grown, if necessary,
     * to accommodate the written bytes, and then the file position is updated
     * with the number of bytes actually written.  Otherwise this method
     * behaves exactly as specified by the {@link WritableByteChannel}
     * interface. </p>
     *
     * @param src the source of the bytes to write into this channel
     * @return the number of bytes actually written
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return byteChannel.write(src);
    }

    /**
     * Writes a sequence of bytes to this channel from a subsequence of the
     * given buffers.
     *
     * <p> Bytes are written starting at this channel's current file position
     * unless the channel is in append mode, in which case the position is
     * first advanced to the end of the file.  The file is grown, if necessary,
     * to accommodate the written bytes, and then the file position is updated
     * with the number of bytes actually written.  Otherwise this method
     * behaves exactly as specified in the {@link GatheringByteChannel}
     * interface.  </p>
     *
     * @param srcs the source of the bytes to write into this channel
     * @param offset the index of the first buffer in the buffer array to write bytes from
     * @param length the maximum number of buffers to write bytes from
     * @return the number of bytes actually written
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        int bytesWritten = 0;
        for (int i = offset; i < offset + length; i++) {
            ByteBuffer src = srcs[i];
            bytesWritten += write(src);
        }
        return bytesWritten;
    }

    /**
     * Returns this channel's file position.
     *
     * @return This channel's file position,
     * a non-negative integer counting the number of bytes
     * from the beginning of the file to the current position
     * @throws ClosedChannelException If this channel is closed
     * @throws IOException            If some other I/O error occurs
     */
    @Override
    public long position() throws IOException {
        return byteChannel.position();
    }

    /**
     * Sets this channel's file position.
     *
     * <p> Setting the position to a value that is greater than the file's
     * current size is legal but does not change the size of the file.  A later
     * attempt to read bytes at such a position will immediately return an
     * end-of-file indication.  A later attempt to write bytes at such a
     * position will cause the file to be grown to accommodate the new bytes;
     * the values of any bytes between the previous end-of-file and the
     * newly-written bytes are unspecified.  </p>
     *
     * @param newPosition The new position, a non-negative integer counting
     *                    the number of bytes from the beginning of the file
     * @return This file channel
     * @throws ClosedChannelException   If this channel is closed
     * @throws IllegalArgumentException If the new position is negative
     * @throws IOException              If some other I/O error occurs
     */
    @Override
    public FileChannel position(long newPosition) throws IOException {
        byteChannel.position(newPosition);
        return this;
    }

    /**
     * Returns the current size of this channel's file.
     *
     * @return The current size of this channel's file,
     * measured in bytes
     * @throws ClosedChannelException If this channel is closed
     * @throws IOException            If some other I/O error occurs
     */
    @Override
    public long size() throws IOException {
        return byteChannel.size();
    }

    /**
     * Truncates this channel's file to the given size.
     *
     * <p> If the given size is less than the file's current size then the file
     * is truncated, discarding any bytes beyond the new end of the file.  If
     * the given size is greater than or equal to the file's current size then
     * the file is not modified.  In either case, if this channel's file
     * position is greater than the given size then it is set to that size.
     * </p>
     *
     * @param size The new size, a non-negative byte count
     * @return This file channel
     * @throws NonWritableChannelException If this channel was not opened for writing
     * @throws ClosedChannelException      If this channel is closed
     * @throws IllegalArgumentException    If the new size is negative
     * @throws IOException                 If some other I/O error occurs
     */
    @Override
    public FileChannel truncate(long size) throws IOException {
        byteChannel.truncate(size);
        return this;
    }

    /**
     * Forces any updates to this channel's file to be written to the storage
     * device that contains it.
     *
     * <p> If this channel's file resides on a local storage device then when
     * this method returns it is guaranteed that all changes made to the file
     * since this channel was created, or since this method was last invoked,
     * will have been written to that device.  This is useful for ensuring that
     * critical information is not lost in the event of a system crash.
     *
     * <p> If the file does not reside on a local device then no such guarantee
     * is made.
     *
     * <p> The {@code metaData} parameter can be used to limit the number of
     * I/O operations that this method is required to perform.  Passing
     * {@code false} for this parameter indicates that only updates to the
     * file's content need be written to storage; passing {@code true}
     * indicates that updates to both the file's content and metadata must be
     * written, which generally requires at least one more I/O operation.
     * Whether this parameter actually has any effect is dependent upon the
     * underlying operating system and is therefore unspecified.
     *
     * <p> Invoking this method may cause an I/O operation to occur even if the
     * channel was only opened for reading.  Some operating systems, for
     * example, maintain a last-access time as part of a file's metadata, and
     * this time is updated whenever the file is read.  Whether or not this is
     * actually done is system-dependent and is therefore unspecified.
     *
     * <p> This method is only guaranteed to force changes that were made to
     * this channel's file via the methods defined in this class.  It may or
     * may not force changes that were made by modifying the content of a
     * {@link MappedByteBuffer <i>mapped byte buffer</i>} obtained by
     * invoking the {@link #map map} method.  Invoking the {@link
     * MappedByteBuffer#force force} method of the mapped byte buffer will
     * force changes made to the buffer's content to be written.  </p>
     *
     * @param metaData If {@code true} then this method is required to force changes
     *                 to both the file's content and metadata to be written to
     *                 storage; otherwise, it need only force content changes to be
     *                 written
     * @throws ClosedChannelException If this channel is closed
     * @throws IOException            If some other I/O error occurs
     */
    @Override
    public void force(boolean metaData) throws IOException {
        if (byteChannel.getWriteDelegate() != null) {
            ((S3WritableByteChannel) byteChannel.getWriteDelegate()).force();
        }
    }

    /**
     * Transfers bytes from this channel's file to the given writable byte
     * channel.
     *
     * <p> An attempt is made to read up to {@code count} bytes starting at
     * the given {@code position} in this channel's file and write them to the
     * target channel.  An invocation of this method may or may not transfer
     * all of the requested bytes; whether or not it does so depends upon the
     * natures and states of the channels.  Fewer than the requested number of
     * bytes are transferred if this channel's file contains fewer than
     * {@code count} bytes starting at the given {@code position}, or if the
     * target channel is non-blocking and it has fewer than {@code count}
     * bytes free in its output buffer.
     *
     * <p> This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then no bytes are
     * transferred.  If the target channel has a position then bytes are
     * written starting at that position and then the position is incremented
     * by the number of bytes written.
     *
     * <p> This method is potentially much more efficient than a simple loop
     * that reads from this channel and writes to the target channel.  Many
     * operating systems can transfer bytes directly from the filesystem cache
     * to the target channel without actually copying them.  </p>
     *
     * @param position The position within the file at which the transfer is to begin;
     *                 must be non-negative
     * @param count    The maximum number of bytes to be transferred; must be
     *                 non-negative
     * @param target   The target channel
     * @return The number of bytes, possibly zero,
     * that were actually transferred
     * @throws IllegalArgumentException    If the preconditions on the parameters do not hold
     * @throws NonReadableChannelException If this channel was not opened for reading
     * @throws NonWritableChannelException If the target channel was not opened for writing
     * @throws ClosedChannelException      If either this channel or the target channel is closed
     * @throws AsynchronousCloseException  If another thread closes either channel
     *                                     while the transfer is in progress
     * @throws ClosedByInterruptException  If another thread interrupts the current thread while the
     *                                     transfer is in progress, thereby closing both channels and
     *                                     setting the current thread's interrupt status
     * @throws IOException                 If some other I/O error occurs
     */
    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative");
        }

        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative");
        }

        if (count == 0 || position > byteChannel.size()) {
            return 0;  // no op
        }

        if (byteChannel.getReadDelegate() == null) {
            throw new NonReadableChannelException();
        }

        if (!byteChannel.isOpen()) {
            throw new ClosedChannelException();
        }

        synchronized (byteChannel) {
            var originalPosition = this.byteChannel.position();
            this.byteChannel.position(position);
            long bytesTransferred = 0;

            while (bytesTransferred < count) {
                int bytesToTransfer = ((int) Math.min(count - bytesTransferred, Integer.MAX_VALUE));
                var buffer = ByteBuffer.allocate(bytesToTransfer);
                int bytesRead = this.byteChannel.read(buffer);
                if (bytesRead == -1) {
                    break;
                }
                buffer.flip();
                target.write(buffer);
                bytesTransferred += bytesRead;
            }

            // set the byte buffers position back to this channels position
            this.byteChannel.position(originalPosition);
            return bytesTransferred;
        }
    }

    /**
     * Transfers bytes into this channel's file from the given readable byte
     * channel.
     *
     * <p> An attempt is made to read up to {@code count} bytes from the
     * source channel and write them to this channel's file starting at the
     * given {@code position}.  An invocation of this method may or may not
     * transfer all of the requested bytes; whether or not it does so depends
     * upon the natures and states of the channels.  Fewer than the requested
     * number of bytes will be transferred if the source channel has fewer than
     * {@code count} bytes remaining, or if the source channel is non-blocking
     * and has fewer than {@code count} bytes immediately available in its
     * input buffer.
     *
     * <p> This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then no bytes are
     * transferred.  If the source channel has a position then bytes are read
     * starting at that position and then the position is incremented by the
     * number of bytes read.
     *
     * <p> This method is potentially much more efficient than a simple loop
     * that reads from the source channel and writes to this channel.  Many
     * operating systems can transfer bytes directly from the source channel
     * into the filesystem cache without actually copying them.  </p>
     *
     * @param src      The source channel
     * @param position The position within the file at which the transfer is to begin;
     *                 must be non-negative
     * @param count    The maximum number of bytes to be transferred; must be
     *                 non-negative
     * @return The number of bytes, possibly zero,
     * that were actually transferred
     * @throws IllegalArgumentException    If the preconditions on the parameters do not hold
     * @throws NonReadableChannelException If the source channel was not opened for reading
     * @throws NonWritableChannelException If this channel was not opened for writing
     * @throws ClosedChannelException      If either this channel or the source channel is closed
     * @throws AsynchronousCloseException  If another thread closes either channel
     *                                     while the transfer is in progress
     * @throws ClosedByInterruptException  If another thread interrupts the current thread while the
     *                                     transfer is in progress, thereby closing both channels and
     *                                     setting the current thread's interrupt status
     * @throws IOException                 If some other I/O error occurs
     */
    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("file position must be non-negative");
        }

        if (count < 0) {
            throw new IllegalArgumentException("byte count must be non-negative");
        }

        if (count == 0 || position > byteChannel.size()) {
            return 0;  // no op
        }

        if (byteChannel.getWriteDelegate() == null) {
            throw new NonWritableChannelException();
        }

        if (!byteChannel.isOpen()) {
            throw new ClosedChannelException();
        }
        synchronized (byteChannel) {
            var originalPosition = byteChannel.position();
            byteChannel.position(position);
            long bytesTransferred = 0;

            while (bytesTransferred < count) {
                int bytesToTransfer = ((int) Math.min(count - bytesTransferred, Integer.MAX_VALUE));
                var buffer = ByteBuffer.allocate(bytesToTransfer);
                int bytesRead = src.read(buffer);
                if (bytesRead == -1) {
                    break;
                }
                buffer.flip();
                byteChannel.write(buffer);
                bytesTransferred += bytesRead;
            }

            // set the byte buffers position back to this channels position
            byteChannel.position(originalPosition);
            return bytesTransferred;
        }
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer,
     * starting at the given file position.
     *
     * <p> This method works in the same manner as the {@link
     * #read(ByteBuffer)} method, except that bytes are read starting at the
     * given file position rather than at the channel's current position.  This
     * method does not modify this channel's position.  If the given position
     * is greater than the file's current size then no bytes are read.  </p>
     *
     * @param dst      The buffer into which bytes are to be transferred
     * @param position The file position at which the transfer is to begin;
     *                 must be non-negative
     * @return The number of bytes read, possibly zero, or {@code -1} if the
     * given position is greater than or equal to the file's current
     * size
     * @throws IllegalArgumentException    If the position is negative
     * @throws NonReadableChannelException If this channel was not opened for reading
     * @throws ClosedChannelException      If this channel is closed
     * @throws AsynchronousCloseException  If another thread closes this channel
     *                                     while the read operation is in progress
     * @throws ClosedByInterruptException  If another thread interrupts the current thread
     *                                     while the read operation is in progress, thereby
     *                                     closing the channel and setting the current thread's
     *                                     interrupt status
     * @throws IOException                 If some other I/O error occurs
     */
    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("file position must be non-negative");
        }
        byteChannel.position(position);
        return byteChannel.read(dst);
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer,
     * starting at the given file position.
     *
     * <p> This method works in the same manner as the {@link
     * #write(ByteBuffer)} method, except that bytes are written starting at
     * the given file position rather than at the channel's current position.
     * This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then the file will be
     * grown to accommodate the new bytes; the values of any bytes between the
     * previous end-of-file and the newly-written bytes are unspecified.  </p>
     *
     * @param src      The buffer from which bytes are to be transferred
     * @param position The file position at which the transfer is to begin;
     *                 must be non-negative
     * @return The number of bytes written, possibly zero
     * @throws IllegalArgumentException    If the position is negative
     * @throws NonWritableChannelException If this channel was not opened for writing
     * @throws ClosedChannelException      If this channel is closed
     * @throws AsynchronousCloseException  If another thread closes this channel
     *                                     while the write operation is in progress
     * @throws ClosedByInterruptException  If another thread interrupts the current thread
     *                                     while the write operation is in progress, thereby
     *                                     closing the channel and setting the current thread's
     *                                     interrupt status
     * @throws IOException                 If some other I/O error occurs
     */
    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        synchronized (byteChannel) {
            var originalPosition = byteChannel.position();
            byteChannel.position(position);
            var bytesWritten = byteChannel.write(src);
            byteChannel.position(originalPosition);
            return bytesWritten;
        }
    }

    /**
     * This method is not supported by this implementation, and the
     * {@link IOException} thrown always includes the message "This library current doesn't support MappedByteBuffers".
     *
     * @param mode
     *         One of the constants {@link MapMode#READ_ONLY READ_ONLY}, {@link
     *         MapMode#READ_WRITE READ_WRITE}, or {@link MapMode#PRIVATE
     *         PRIVATE} defined in the {@link MapMode} class, according to
     *         whether the file is to be mapped read-only, read/write, or
     *         privately (copy-on-write), respectively
     *
     * @param position
     *         The position within the file at which the mapped region
     *         is to start; must be non-negative
     *
     * @param size
     *         The size of the region to be mapped; must be non-negative and
     *         no greater than {@link java.lang.Integer#MAX_VALUE}
     *
     * @return Never returns, always throws an exception
     * @throws IOException Always throws an exception
     */
    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new IOException(new NotYetImplementedException("This library current doesn't support MappedByteBuffers"));
    }

    /**
     * Acquires a lock on the given region of this channel's file.
     *
     * <p> An invocation of this method will block until the region can be
     * locked, this channel is closed, or the invoking thread is interrupted,
     * whichever comes first.
     *
     * <p> If this channel is closed by another thread during an invocation of
     * this method then an {@link AsynchronousCloseException} will be thrown.
     *
     * <p> If the invoking thread is interrupted while waiting to acquire the
     * lock then its interrupt status will be set and a {@link
     * FileLockInterruptionException} will be thrown.  If the invoker's
     * interrupt status is set when this method is invoked then that exception
     * will be thrown immediately; the thread's interrupt status will not be
     * changed.
     *
     * <p> The region specified by the {@code position} and {@code size}
     * parameters need not be contained within, or even overlap, the actual
     * underlying file.  Lock regions are fixed in size; if a locked region
     * initially contains the end of the file and the file grows beyond the
     * region then the new portion of the file will not be covered by the lock.
     * If a file is expected to grow in size and a lock on the entire file is
     * required then a region starting at zero, and no smaller than the
     * expected maximum size of the file, should be locked.  The zero-argument
     * {@link #lock()} method simply locks a region of size {@link
     * Long#MAX_VALUE}.
     *
     * <p> Some operating systems do not support shared locks, in which case a
     * request for a shared lock is automatically converted into a request for
     * an exclusive lock.  Whether the newly-acquired lock is shared or
     * exclusive may be tested by invoking the resulting lock object's {@link
     * FileLock#isShared() isShared} method.
     *
     * <p> File locks are held on behalf of the entire Java virtual machine.
     * They are not suitable for controlling access to a file by multiple
     * threads within the same virtual machine.  </p>
     *
     * @param position The position at which the locked region is to start; must be
     *                 non-negative
     * @param size     The size of the locked region; must be non-negative, and the sum
     *                 {@code position}&nbsp;+&nbsp;{@code size} must be non-negative
     * @param shared   {@code true} to request a shared lock, in which case this
     *                 channel must be open for reading (and possibly writing);
     *                 {@code false} to request an exclusive lock, in which case this
     *                 channel must be open for writing (and possibly reading)
     * @return A lock object representing the newly-acquired lock
     * @throws IllegalArgumentException      If the preconditions on the parameters do not hold
     * @throws ClosedChannelException        If this channel is closed
     * @throws AsynchronousCloseException    If another thread closes this channel while the invoking
     *                                       thread is blocked in this method
     * @throws FileLockInterruptionException If the invoking thread is interrupted while blocked in this
     *                                       method
     * @throws OverlappingFileLockException  If a lock that overlaps the requested region is already held by
     *                                       this Java virtual machine, or if another thread is already
     *                                       blocked in this method and is attempting to lock an overlapping
     *                                       region
     * @throws NonReadableChannelException   If {@code shared} is {@code true} this channel was not
     *                                       opened for reading
     * @throws NonWritableChannelException   If {@code shared} is {@code false} but this channel was not
     *                                       opened for writing
     * @throws IOException                   If some other I/O error occurs
     * @see #lock()
     * @see #tryLock()
     * @see #tryLock(long, long, boolean)
     */
    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new IOException(new UnsupportedOperationException("S3 does not support file locks"));
    }

    /**
     * Attempts to acquire a lock on the given region of this channel's file.
     *
     * <p> This method does not block.  An invocation always returns
     * immediately, either having acquired a lock on the requested region or
     * having failed to do so.  If it fails to acquire a lock because an
     * overlapping lock is held by another program then it returns
     * {@code null}.  If it fails to acquire a lock for any other reason then
     * an appropriate exception is thrown.
     *
     * <p> The region specified by the {@code position} and {@code size}
     * parameters need not be contained within, or even overlap, the actual
     * underlying file.  Lock regions are fixed in size; if a locked region
     * initially contains the end of the file and the file grows beyond the
     * region then the new portion of the file will not be covered by the lock.
     * If a file is expected to grow in size and a lock on the entire file is
     * required then a region starting at zero, and no smaller than the
     * expected maximum size of the file, should be locked.  The zero-argument
     * {@link #tryLock()} method simply locks a region of size {@link
     * Long#MAX_VALUE}.
     *
     * <p> Some operating systems do not support shared locks, in which case a
     * request for a shared lock is automatically converted into a request for
     * an exclusive lock.  Whether the newly-acquired lock is shared or
     * exclusive may be tested by invoking the resulting lock object's {@link
     * FileLock#isShared() isShared} method.
     *
     * <p> File locks are held on behalf of the entire Java virtual machine.
     * They are not suitable for controlling access to a file by multiple
     * threads within the same virtual machine.  </p>
     *
     * @param position The position at which the locked region is to start; must be
     *                 non-negative
     * @param size     The size of the locked region; must be non-negative, and the sum
     *                 {@code position}&nbsp;+&nbsp;{@code size} must be non-negative
     * @param shared   {@code true} to request a shared lock,
     *                 {@code false} to request an exclusive lock
     * @return A lock object representing the newly-acquired lock,
     * or {@code null} if the lock could not be acquired
     * because another program holds an overlapping lock
     * @throws IllegalArgumentException     If the preconditions on the parameters do not hold
     * @throws ClosedChannelException       If this channel is closed
     * @throws OverlappingFileLockException If a lock that overlaps the requested region is already held by
     *                                      this Java virtual machine, or if another thread is already
     *                                      blocked in this method and is attempting to lock an overlapping
     *                                      region of the same file
     * @throws IOException                  If some other I/O error occurs
     * @see #lock()
     * @see #lock(long, long, boolean)
     * @see #tryLock()
     */
    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new IOException(new UnsupportedOperationException("S3 does not support file locks"));
    }

    /**
     * Closes this channel.
     *
     * <p> This method is invoked by the {@link #close close} method in order
     * to perform the actual work of closing the channel.  This method is only
     * invoked if the channel has not yet been closed, and it is never invoked
     * more than once.
     *
     * <p> An implementation of this method must arrange for any other thread
     * that is blocked in an I/O operation upon this channel to return
     * immediately, either by throwing an exception or by returning normally.
     * </p>
     *
     * @throws IOException If an I/O error occurs while closing the channel
     */
    @Override
    protected void implCloseChannel() throws IOException {
        byteChannel.close();
    }
}
