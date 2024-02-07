/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3;

import java.net.URI;
import java.util.concurrent.Executor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;
import software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration;

/**
 * This class fakes a S3CrtAsyncClientBuilder for testability purposes. It
 * records all configuration passed to the builder in public fields that can
 * easily checked. It then delegates the implementation to the builder created
 * with <code>S3AsyncClient.crtBuilder()</code>
 */
public class FakeAsyncS3ClientBuilder implements S3CrtAsyncClientBuilder {

    private final S3CrtAsyncClientBuilder BUILDER = S3AsyncClient.crtBuilder();

    public AwsCredentialsProvider credentialsProvider = null;
    public Region region = null;
    public Long minimumPartSizeInBytes = null,
                initialReadBufferSizeInBytes = null;
    public Double targetThroughputInGbps = null;
    public Integer maxConcurrency = null;
    public URI endpointOverride = null;
    public Boolean checksumValidationEnabled = null,
                   accelerate = null,
                   forcePathStyle = null;
    public S3CrtHttpConfiguration httpConfiguration = null;
    public S3CrtRetryConfiguration retryConfiguration = null;

    @Override
    public S3CrtAsyncClientBuilder credentialsProvider(AwsCredentialsProvider acp) {
        BUILDER.credentialsProvider(credentialsProvider = acp); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder region(Region r) {
        BUILDER.region(region = r); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder minimumPartSizeInBytes(Long l) {
        BUILDER.minimumPartSizeInBytes(minimumPartSizeInBytes = l); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder targetThroughputInGbps(Double d) {
        BUILDER.targetThroughputInGbps(targetThroughputInGbps = d); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder maxConcurrency(Integer i) {
        BUILDER.maxConcurrency(maxConcurrency = i); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder endpointOverride(URI u) {
        BUILDER.endpointOverride(endpointOverride = u); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder checksumValidationEnabled(Boolean b) {
        BUILDER.checksumValidationEnabled(checksumValidationEnabled = b);
        return this;
    }

    @Override
    public S3CrtAsyncClientBuilder initialReadBufferSizeInBytes(Long l) {
        BUILDER.initialReadBufferSizeInBytes(initialReadBufferSizeInBytes = l);
        return this;
    }

    @Override
    public S3CrtAsyncClientBuilder httpConfiguration(S3CrtHttpConfiguration c) {
        BUILDER.httpConfiguration(httpConfiguration = c); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder retryConfiguration(S3CrtRetryConfiguration c) {
        BUILDER.retryConfiguration(retryConfiguration = c);  return this;
    }

    @Override
    public S3CrtAsyncClientBuilder accelerate(Boolean b) {
        BUILDER.accelerate(accelerate = b); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder forcePathStyle(Boolean b) {
        BUILDER.forcePathStyle(forcePathStyle = b); return this;
    }

    @Override
    public S3AsyncClient build() {
        return BUILDER.build();
    }

    @Override
    public S3CrtAsyncClientBuilder crossRegionAccessEnabled(Boolean b) {
        BUILDER.crossRegionAccessEnabled(b); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder thresholdInBytes(Long l) {
        BUILDER.thresholdInBytes(l); return this;
    }

    @Override
    public S3CrtAsyncClientBuilder futureCompletionExecutor(Executor executor) {
        BUILDER.futureCompletionExecutor(executor);
        return this;
    }
}
