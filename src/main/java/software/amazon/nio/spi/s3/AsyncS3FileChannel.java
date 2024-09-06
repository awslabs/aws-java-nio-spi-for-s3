/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import software.amazon.nio.spi.s3.util.TimeOutUtils;

public class AsyncS3FileChannel extends AsynchronousFileChannel {

    private final S3SeekableByteChannel byteChannel;

    AsyncS3FileChannel(S3SeekableByteChannel byteChannel) {
        this.byteChannel = byteChannel;
    }

    @Override
    public long size() throws IOException {
        return byteChannel.size();
    }

    @Override
    public AsynchronousFileChannel truncate(long size) throws IOException {
        byteChannel.truncate(size);
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        if (byteChannel.getWriteDelegate() != null) {
            ((S3WritableByteChannel) byteChannel.getWriteDelegate()).force();
        }
    }

    @Override
    public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler) {
        throw new UnsupportedOperationException("S3 does not support file locking");
    }

    @Override
    public Future<FileLock> lock(long position, long size, boolean shared) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("S3 does not support file locking"));
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new IOException(new UnsupportedOperationException("S3 does not support file locking"));
    }

    @Override
    public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        Future<Integer> future =  read(dst, position);
        try {
            handler.completed(future.get(TimeOutUtils.TIMEOUT_TIME_LENGTH_5, TimeUnit.MINUTES), attachment);
        } catch (Exception e) {
            handler.failed(e, attachment);
        }
    }

    @Override
    public Future<Integer> read(ByteBuffer dst, long position) {
        if (position < 0) {
            throw new IllegalArgumentException("position: " + position);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                byteChannel.position(position);
                return byteChannel.read(dst);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        Future<Integer> future = write(src, position);
        try {
            handler.completed(future.get(TimeOutUtils.TIMEOUT_TIME_LENGTH_5, TimeUnit.MINUTES), attachment);
        } catch (Exception e) {
            handler.failed(e, attachment);
        }
    }

    @Override
    public Future<Integer> write(ByteBuffer src, long position) {
        if (position < 0) {
            throw new IllegalArgumentException("position: " + position);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                byteChannel.position(position);
                return byteChannel.write(src);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public boolean isOpen() {
        return byteChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        byteChannel.close();
    }
}
