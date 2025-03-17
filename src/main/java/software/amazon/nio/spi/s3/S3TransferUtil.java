/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

final class S3TransferUtil {
    private final S3ObjectIntegrityCheck integrityCheck;
    private final S3AsyncClient client;
    private final Long timeout;
    private final TimeUnit timeUnit;

    S3TransferUtil(S3AsyncClient client, Long timeout, TimeUnit timeUnit, S3ObjectIntegrityCheck integrityCheck) {
        this.client = client;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.integrityCheck = integrityCheck;
    }

    void downloadToLocalFile(S3Path path, Path destination, S3OpenOption... options)
            throws InterruptedException, ExecutionException, TimeoutException {
        var getObjectRequest = GetObjectRequest.builder()
            .bucket(path.bucketName())
            .key(path.getKey());
        for (var option : options) {
            option.apply(getObjectRequest);
        }
        var transformerConfig = FileTransformerConfiguration.defaultCreateOrReplaceExisting();
        var responseTransformer = AsyncResponseTransformer.<GetObjectResponse>toFile(destination, transformerConfig);
        var downloadCompletableFuture = client.getObject(getObjectRequest.build(), responseTransformer)
            .toCompletableFuture();

        if (timeout != null && timeUnit != null) {
            downloadCompletableFuture.get(timeout, timeUnit);
        } else {
            downloadCompletableFuture.join();
        }
    }

    void uploadLocalFile(S3Path path, Path localFile) throws IOException {
        try (var s3TransferManager = S3TransferManager.builder().s3Client(client).build()) {
            var putObjectRequest = PutObjectRequest.builder()
                .bucket(path.bucketName())
                .key(path.getKey())
                .contentType(Files.probeContentType(localFile));
            integrityCheck.addChecksumToRequest(localFile, putObjectRequest);
            var uploadCompletableFuture = s3TransferManager.uploadFile(
                UploadFileRequest.builder()
                    .putObjectRequest(putObjectRequest.build())
                    .source(localFile)
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
    }
}
