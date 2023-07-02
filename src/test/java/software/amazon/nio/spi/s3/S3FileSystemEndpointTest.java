/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_REGION_PROPERTY;

@ExtendWith(MockitoExtension.class)
public class S3FileSystemEndpointTest {

    final private FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();
    final private Map<String, String> CONFIG = new HashMap<>();

    {
        CONFIG.put(AWS_REGION_PROPERTY, "us-east-1");
    }

    S3FileSystemProvider provider;

    @BeforeEach
    public void init() {
        provider = new S3FileSystemProvider();
    }

    @Test
    public void clientWithProvidedEndpoint() throws Exception {
        final String URI1 = "s3://endpoint2.io:8080/bucket/resource";

        //
        // For non AWS S3 buckets, backet's region is not discovered runtime and
        // it must be provided in the AWS profile
        //
        S3FileSystem fs = new S3FileSystem(S3URI.of(URI.create(URI1)), provider, new S3NioSpiConfiguration(CONFIG));
        fs.clientProvider.asyncClientBuilder = BUILDER;

        S3AsyncClient client = fs.client();
        assertEquals(URI.create("https://endpoint2.io:8080"), BUILDER.endpointOverride);
        assertNull(BUILDER.credentialsProvider);
    }

    @Test
    public void clientWithProvidedEndpointAndCredentials() throws Exception {
        final String URI1 = "s3://key2:secret2@endpoint2.io:8080/bucket/resource";

        //
        // For non AWS S3 buckets, backet's region is not discovered runtime
        // and it must be provided in the AWS profile
        //
        S3FileSystem fs = new S3FileSystem(S3URI.of(URI.create(URI1)), provider, new S3NioSpiConfiguration(CONFIG));
        fs.clientProvider.asyncClientBuilder = BUILDER;

        fs.client();
        assertEquals(URI.create("https://endpoint2.io:8080"), BUILDER.endpointOverride);
        assertNotNull(BUILDER.credentialsProvider);
        assertEquals("key2", BUILDER.credentialsProvider.resolveCredentials().accessKeyId());
        assertEquals("secret2", BUILDER.credentialsProvider.resolveCredentials().secretAccessKey());
    }

}