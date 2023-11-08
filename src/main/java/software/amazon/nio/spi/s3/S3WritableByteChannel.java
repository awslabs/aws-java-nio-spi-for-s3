package software.amazon.nio.spi.s3;

import org.slf4j.Logger;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class S3WritableByteChannel implements WritableByteChannel {
    private final S3AsyncClient client;
    private final S3Path path;
    private final Path tempFile;
    private final SeekableByteChannel channel;
    private final Long timeout;
    private final TimeUnit timeUnit;

    private boolean open;

    Logger logger = org.slf4j.LoggerFactory.getLogger(S3WritableByteChannel.class);

    S3WritableByteChannel(S3Path path, S3AsyncClient client, Set<? extends OpenOption> options, Long timeout, TimeUnit timeUnit) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(client);
        this.timeout = timeout;
        this.timeUnit = timeUnit;

        try {
            var fileSystemProvider = (S3FileSystemProvider) path.getFileSystem().provider();
            var exists = fileSystemProvider.exists(client, path);

            if (exists && options.contains(StandardOpenOption.CREATE_NEW)) {
                throw new FileAlreadyExistsException("File at path:" + path + " already exists");
            }
            if (!exists && !options.contains(StandardOpenOption.CREATE_NEW) && !options.contains(StandardOpenOption.CREATE)) {
                throw new NoSuchFileException("File at path:" + path + " does not exist yet");
            }

            tempFile = Files.createTempFile("aws-s3-nio-", ".tmp");
            if (exists) {
                try (var s3TransferManager = S3TransferManager.builder().s3Client(client).build()) {
                    var downloadCompletableFuture = s3TransferManager.downloadFile(
                            DownloadFileRequest.builder()
                                    .getObjectRequest(GetObjectRequest.builder()
                                            .bucket(path.bucketName())
                                            .key(path.getKey())
                                            .build())
                                    .destination(tempFile)
                                    .build()
                    ).completionFuture();

                    if (timeout != null && timeUnit != null) {
                        downloadCompletableFuture.get(timeout, timeUnit);
                    } else {
                        downloadCompletableFuture.join();
                    }
                }
            }

            try {
                options.remove(StandardOpenOption.CREATE_NEW);
            } catch (UnsupportedOperationException e) {
                if (options.isEmpty() || !options.contains(StandardOpenOption.CREATE_NEW)) {
                    // options is immutable but it doesn't matter because the value isn't there anyway.
                    logger.debug("Could not remove CREATE_NEW option as the operation is unsupported on options", e);
                } else {
                    throw e;
                }
            }
            channel = Files.newByteChannel(this.tempFile, options);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not open the path:" + path, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new IOException("Could not open the path:" + path, e);
        }

        this.client = client;
        this.path = path;
        this.open = true;
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

        try (var s3TransferManager = S3TransferManager.builder().s3Client(client).build()) {
            var uploadCompletableFuture = s3TransferManager.uploadFile(
                    UploadFileRequest.builder()
                            .putObjectRequest(PutObjectRequest.builder()
                                    .bucket(path.bucketName())
                                    .key(path.getKey())
                                    .contentType(Files.probeContentType(tempFile))
                                    .build())
                            .source(tempFile)
                            .build()
            ).completionFuture();

            if (timeout != null && timeUnit != null) {
                uploadCompletableFuture.get(timeout, timeUnit);
            } else {
                uploadCompletableFuture.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not write to path:" + path, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new IOException("Could not write to path:" + path, e);
        }
        open = false;
    }
}
