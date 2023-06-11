/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3;

import java.net.URI;
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
        credentialsProvider = acp; return BUILDER.credentialsProvider(acp);
    }

    @Override
    public S3CrtAsyncClientBuilder region(Region r) {
        region = r; return BUILDER.region(region);
    }

    @Override
    public S3CrtAsyncClientBuilder minimumPartSizeInBytes(Long l) {
        minimumPartSizeInBytes = l; return BUILDER.minimumPartSizeInBytes(l);
    }

    @Override
    public S3CrtAsyncClientBuilder targetThroughputInGbps(Double d) {
        targetThroughputInGbps = d; return BUILDER.targetThroughputInGbps(d);
    }

    @Override
    public S3CrtAsyncClientBuilder maxConcurrency(Integer i) {
        maxConcurrency = i; return BUILDER.maxConcurrency(i);
    }

    @Override
    public S3CrtAsyncClientBuilder endpointOverride(URI u) {
       endpointOverride = u; return BUILDER.endpointOverride(u);
    }

    @Override
    public S3CrtAsyncClientBuilder checksumValidationEnabled(Boolean b) {
        checksumValidationEnabled = b; return BUILDER.checksumValidationEnabled(b);
    }

    @Override
    public S3CrtAsyncClientBuilder initialReadBufferSizeInBytes(Long l) {
        initialReadBufferSizeInBytes = l; return BUILDER.initialReadBufferSizeInBytes(l);
    }

    @Override
    public S3CrtAsyncClientBuilder httpConfiguration(S3CrtHttpConfiguration c) {
        httpConfiguration = c; return BUILDER.httpConfiguration(c);
    }

    @Override
    public S3AsyncClient build() {
        return BUILDER.build();
    }

    @Override
    public S3CrtAsyncClientBuilder retryConfiguration(S3CrtRetryConfiguration c) {
        retryConfiguration = c; return BUILDER.retryConfiguration(c);
    }

    @Override
    public S3CrtAsyncClientBuilder accelerate(Boolean b) {
        accelerate = b; return BUILDER.accelerate(b);
    }

    @Override
    public S3CrtAsyncClientBuilder forcePathStyle(Boolean b) {
        forcePathStyle = b; return BUILDER.forcePathStyle(b);
    }

}
