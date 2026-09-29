/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

final class S3TransferUtil {
    private final S3AsyncClient client;
    private final Long timeout;
    private final TimeUnit timeUnit;

    S3TransferUtil(S3AsyncClient client, Long timeout, TimeUnit timeUnit) {
        this.client = client;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    void downloadToLocalFile(S3Path path, Path destination, Set<? extends OpenOption> options) throws IOException {
        var s3OpenOptions = S3OpenOption.retainAll(options);
        try {
            if (s3OpenOptions.contains(S3UseTransferManager.INSTANCE)) {
                downloadWithTransferManager(path, destination, s3OpenOptions);
            } else {
                downloadWithGetObject(path, destination, s3OpenOptions);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not read from path: " + path, e);
        } catch (TimeoutException e) {
            throw new IOException("Could not read from path: " + path, e);
        } catch (CompletionException | ExecutionException e) {
            // This complicated download handling is the result of omitting an existence check
            // with a head object request, instead we look for a 404 status code if available.
            var cause = e.getCause();
            if (!(cause instanceof AwsServiceException)) {
                throw new IOException("Could not read from path: " + path, e);
            }
            var s3e = (AwsServiceException) cause;
            if (s3e.statusCode() != 404) {
                throw new S3TransferException("GetObject", path, s3e);
            }
            if (!options.contains(StandardOpenOption.CREATE)) {
                throw new NoSuchFileException(path.toString());
            }
            // gracefully handle the file creation
        }
    }

    private void downloadWithGetObject(S3Path path, Path destination, Set<S3OpenOption> options)
            throws InterruptedException, ExecutionException, TimeoutException {
        var getObjectRequest = GetObjectRequest.builder()
            .bucket(path.bucketName())
            .key(path.getKey());
        for (var option : options) {
            option.apply(getObjectRequest);
        }
        var transformerConfig = FileTransformerConfiguration.defaultCreateOrReplaceExisting();
        var responseTransformer = AsyncResponseTransformer.<GetObjectResponse>toFile(destination, transformerConfig);
        var downloadCompletableFuture = client.getObject(getObjectRequest.build(), responseTransformer);

        GetObjectResponse getObjectResponse;
        if (timeout != null && timeUnit != null) {
            getObjectResponse = downloadCompletableFuture.get(timeout, timeUnit);
        } else {
            getObjectResponse = downloadCompletableFuture.join();
        }
        for (var option : options) {
            option.consume(getObjectResponse, destination);
        }
    }

    private void downloadWithTransferManager(S3Path path, Path destination, Set<S3OpenOption> options)
            throws InterruptedException, ExecutionException, TimeoutException {
        try (var transferManager = S3TransferManager.builder().s3Client(client).build();) {
            var getObjectRequest = GetObjectRequest.builder()
                .bucket(path.bucketName())
                .key(path.getKey());
            for (var option : options) {
                option.apply(getObjectRequest);
            }
            var downloadRequest = DownloadFileRequest.builder()
                .getObjectRequest(getObjectRequest.build())
                .destination(destination)
                .build();
            var future = transferManager.downloadFile(downloadRequest)
                .completionFuture();
            CompletedFileDownload completedFileDownload;
            if (timeout != null && timeUnit != null) {
                completedFileDownload = future.get(timeout, timeUnit);
            } else {
                completedFileDownload = future.join();
            }
            for (var option : options) {
                option.consume(completedFileDownload.response(), destination);
            }
        }
    }

    void uploadLocalFile(S3Path path, Path localFile, Set<? extends OpenOption> options) throws IOException {
        var s3OpenOptions = S3OpenOption.retainAll(options);
        for (var option : s3OpenOptions) {
            if (option.preventPutObjectRequest(localFile)) {
                return;
            }
        }
        try {
            if (s3OpenOptions.contains(S3UseTransferManager.INSTANCE)) {
                uploadWithTransferManager(path, localFile, s3OpenOptions);
            } else {
                uploadWithPutObject(path, localFile, s3OpenOptions);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not write to path: " + path, e);
        } catch (TimeoutException e) {
            throw new IOException("Could not write to path: " + path, e);
        } catch (CompletionException | ExecutionException e) {
            var cause = e.getCause();
            if (!(cause instanceof AwsServiceException)) {
                throw new IOException("Could not write to path: " + path, e);
            }
            var s3e = (AwsServiceException) cause;
            throw new S3TransferException("PutObject", path, s3e);
        }
    }

    private void uploadWithPutObject(S3Path path, Path localFile, Set<S3OpenOption> options)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        var putObjectRequest = PutObjectRequest.builder()
            .bucket(path.bucketName())
            .key(path.getKey())
            .contentType(Files.probeContentType(localFile));
        for (var option : options) {
            option.apply(putObjectRequest, localFile);
        }
        var uploadCompletableFuture = client.putObject(putObjectRequest.build(), AsyncRequestBody.fromFile(localFile));

        PutObjectResponse putObjectResponse;
        if (timeout != null && timeUnit != null) {
            putObjectResponse = uploadCompletableFuture.get(timeout, timeUnit);
        } else {
            putObjectResponse = uploadCompletableFuture.join();
        }
        for (var option : options) {
            option.consume(putObjectResponse, localFile);
        }
    }

    private void uploadWithTransferManager(S3Path path, Path localFile, Set<S3OpenOption> options)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (var transferManager = S3TransferManager.builder().s3Client(client).build()) {
            var putObjectRequest = PutObjectRequest.builder()
                .bucket(path.bucketName())
                .key(path.getKey())
                .contentType(Files.probeContentType(localFile));
            for (var option : options) {
                option.apply(putObjectRequest, localFile);
            }
            var uploadCompletableFuture = transferManager.uploadFile(UploadFileRequest.builder()
                .putObjectRequest(putObjectRequest.build())
                .source(localFile)
                .build())
                .completionFuture();

            CompletedFileUpload putObjectResponse;
            if (timeout != null && timeUnit != null) {
                putObjectResponse = uploadCompletableFuture.get(timeout, timeUnit);
            } else {
                putObjectResponse = uploadCompletableFuture.join();
            }
            for (var option : options) {
                option.consume(putObjectResponse.response(), localFile);
            }
        }
    }
}
