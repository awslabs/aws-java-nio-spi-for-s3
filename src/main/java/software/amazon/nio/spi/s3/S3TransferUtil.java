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
import java.util.stream.Stream;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.FileTransformerConfiguration;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

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

    void downloadToLocalFile(S3Path path, Path destination, Set<? extends OpenOption> options) throws IOException {
        var s3OpenOptions = options.stream()
            .flatMap(o -> o instanceof S3OpenOption
                ? Stream.of((S3OpenOption) o)
                : Stream.empty())
            .toArray(S3OpenOption[]::new);
        try {
            var getObjectRequest = GetObjectRequest.builder()
                .bucket(path.bucketName())
                .key(path.getKey());
            for (var option : s3OpenOptions) {
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
            for (var option : s3OpenOptions) {
                option.consume(getObjectResponse);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not read from path: " + path, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new IOException("Could not read from path: " + path, e);
        } catch (CompletionException e) {
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

    void uploadLocalFile(S3Path path, Path localFile, Set<? extends OpenOption> options) throws IOException {
        var s3OpenOptions = options.stream()
            .flatMap(o -> o instanceof S3OpenOption
                ? Stream.of((S3OpenOption) o)
                : Stream.empty())
            .toArray(S3OpenOption[]::new);
        try {
            var putObjectRequest = PutObjectRequest.builder()
                .bucket(path.bucketName())
                .key(path.getKey())
                .contentType(Files.probeContentType(localFile));
            integrityCheck.addChecksumToRequest(localFile, putObjectRequest);
            for (var option : s3OpenOptions) {
                option.apply(putObjectRequest);
            }
            var uploadCompletableFuture = client.putObject(putObjectRequest.build(), AsyncRequestBody.fromFile(localFile));

            PutObjectResponse putObjectResponse;
            if (timeout != null && timeUnit != null) {
                putObjectResponse = uploadCompletableFuture.get(timeout, timeUnit);
            } else {
                putObjectResponse = uploadCompletableFuture.join();
            }
            for (var option : s3OpenOptions) {
                option.consume(putObjectResponse);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not write to path: " + path, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new IOException("Could not write to path: " + path, e);
        } catch (CompletionException e) {
            var cause = e.getCause();
            if (!(cause instanceof AwsServiceException)) {
                throw new IOException("Could not write to path: " + path, e);
            }
            var s3e = (AwsServiceException) cause;
            throw new S3TransferException("PutObject", path, s3e);
        }
    }
}
