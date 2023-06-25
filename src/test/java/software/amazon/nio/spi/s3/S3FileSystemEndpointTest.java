/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import java.net.URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@ExtendWith(MockitoExtension.class)
public class S3FileSystemEndpointTest {

    final private FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();

    S3FileSystemProvider provider;

    @BeforeEach
    public void init() {
        provider = new S3FileSystemProvider();
    }

    @Test
    public void clientWithProvidedEndpoint() throws Exception {
        final String URI1 = "s3://endpoint1.io/bucket/resource";
        final String URI2 = "s3://endpoint2.io:8080/bucket/resource";

        //
        // For non AWS S3 buckets, backet's region is not discovered runtime and it
        // must be provided in the AWS profile
        //
        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "aws-east-1");

            S3FileSystem fs = new S3FileSystem(URI.create(URI1), provider);
            fs.clientProvider.asyncClientBuilder = BUILDER;

            S3AsyncClient client = fs.client();
            assertEquals(URI.create("https://endpoint1.io"), BUILDER.endpointOverride);
            assertNull(BUILDER.credentialsProvider);

            fs = new S3FileSystem(URI.create(URI2), provider);
            fs.clientProvider.asyncClientBuilder = BUILDER;

            fs.client();
            assertEquals(URI.create("https://endpoint2.io:8080"), BUILDER.endpointOverride);
            assertNull(BUILDER.credentialsProvider);
        });
    }

    @Test
    public void clientWithProvidedEndpointAndCredentials() throws Exception {
        final String URI1 = "s3://key1:secret1@endpoint1.io/bucket/resource";
        final String URI2 = "s3://key2:secret2@endpoint2.io:8080/bucket/resource";

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "aws-east-1");

            S3FileSystem fs = new S3FileSystem(URI.create(URI1), provider);
            fs.clientProvider.asyncClientBuilder = BUILDER;

            //
            // For non AWS S3 buckets, backet's region is not discovered runtime and it
            // must be provided in the AWS profile
            //
            S3AsyncClient client = fs.client();

            assertEquals(URI.create("https://endpoint1.io"), BUILDER.endpointOverride);
            assertNotNull(BUILDER.credentialsProvider);
            assertEquals("key1", BUILDER.credentialsProvider.resolveCredentials().accessKeyId());
            assertEquals("secret1", BUILDER.credentialsProvider.resolveCredentials().secretAccessKey());

            fs = new S3FileSystem(URI.create(URI2), provider);
            fs.clientProvider.asyncClientBuilder = BUILDER;

            fs.client();
            assertEquals(URI.create("https://endpoint2.io:8080"), BUILDER.endpointOverride);
            assertNotNull(BUILDER.credentialsProvider);
            assertEquals("key2", BUILDER.credentialsProvider.resolveCredentials().accessKeyId());
            assertEquals("secret2", BUILDER.credentialsProvider.resolveCredentials().secretAccessKey());
        });
    }

}