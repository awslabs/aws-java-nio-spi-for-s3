/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3WritableByteChannel implements SeekableByteChannel {
    private final S3Path path;
    private final Path tempFile;
    private final SeekableByteChannel channel;
    private final S3TransferUtil s3TransferUtil;

    private boolean open;

    S3WritableByteChannel(
        S3Path path,
        S3AsyncClient client,
        S3TransferUtil s3TransferUtil,
        Set<? extends OpenOption> options
    ) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(client);
        this.s3TransferUtil = s3TransferUtil;
        this.path = path;

        try {
            var fileSystemProvider = (S3FileSystemProvider) path.getFileSystem().provider();

            if (options.contains(StandardOpenOption.CREATE_NEW) && fileSystemProvider.exists(client, path)) {
                throw new FileAlreadyExistsException("File at path:" + path + " already exists");
            }

            tempFile = path.getFileSystem().createTempFile(path);
            // this complicated download handling is the result of
            // avoiding an existence check with a head-object request
            if (!options.contains(StandardOpenOption.CREATE_NEW)) {
                try {
                    s3TransferUtil.downloadToLocalFile(path, tempFile);
                } catch (CompletionException e) {
                    var cause = e.getCause();
                    if (!(cause instanceof S3Exception)) {
                        throw e;
                    }
                    var s3e = (S3Exception) cause;
                    if (s3e.statusCode() != 404) {
                        throw e;
                    }
                    if (!options.contains(StandardOpenOption.CREATE)) {
                        throw new NoSuchFileException("File at path " + path + " does not exist yet");
                    }
                    // gracefully handle the file creation
                }
            }

            channel = Files.newByteChannel(this.tempFile, removeCreateNew(options));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not open the path:" + path, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new IOException("Could not open the path:" + path, e);
        }
        this.open = true;
    }

    private @NonNull Set<? extends OpenOption> removeCreateNew(Set<? extends OpenOption> options) {
        var auxOptions = new HashSet<>(options);
        auxOptions.remove(StandardOpenOption.CREATE_NEW);
        return Set.copyOf(auxOptions);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return channel.write(src);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        if (!open) {
            // channel has already been closed -> close() should have no effect
            return;
        }

        s3TransferUtil.uploadLocalFile(path, tempFile);
        Files.deleteIfExists(tempFile);

        open = false;
    }

    /**
     * Cause the local tmp data to be written to S3 without closing the channel and without deleting the tmp file.
     * @throws IOException if an error occurs during the upload
     * @throws ClosedChannelException if the channel is closed
     */
    protected void force() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }
        s3TransferUtil.uploadLocalFile(path, tempFile);
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        channel.position(newPosition);
        return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException("Currently not supported");
    }
}
