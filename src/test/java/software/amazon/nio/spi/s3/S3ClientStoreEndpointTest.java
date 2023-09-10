/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import java.net.URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@SuppressWarnings("unchecked")
@Deprecated
public class S3ClientStoreEndpointTest {


    @Test
    public void testAsyncClientWithProvidedEndpointAndCredentials() throws Exception {
        final String ENDPOINT1 = "endpoint1.io:1010";
        final String ENDPOINT2 = "endpoint2.io:2020";
        final String BUCKET1 = "bucket1";
        final String BUCKET2 = "bucket2";
        final AwsCredentials CREDENTIALS1 = AwsBasicCredentials.create("key1", "secret1");
        final AwsCredentials CREDENTIALS2 = AwsBasicCredentials.create("key2", "secret2");

        final FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();

        S3ClientStore cs = S3ClientStore.getInstance();
        cs.provider.asyncClientBuilder = BUILDER;

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "aws-east-1");
            cs.provider.configuration.withEndpoint(ENDPOINT1).withCredentials(CREDENTIALS1);
            cs.provider.generateAsyncClient(BUCKET1);
        });

        assertEquals(URI.create("https://endpoint1.io:1010"), BUILDER.endpointOverride);
        assertTrue(BUILDER.credentialsProvider instanceof AwsCredentialsProvider);
        AwsCredentialsProvider credentials = (AwsCredentialsProvider)BUILDER.credentialsProvider;
        assertEquals("key1", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret1", credentials.resolveCredentials().secretAccessKey());

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "aws-east-1");
            cs.provider.configuration.withEndpoint(ENDPOINT2).withCredentials(CREDENTIALS2);
            cs.provider.generateAsyncClient(BUCKET2);
        });

        assertEquals(URI.create("https://endpoint2.io:2020"), BUILDER.endpointOverride);
        assertTrue(BUILDER.credentialsProvider instanceof AwsCredentialsProvider);
        credentials = (AwsCredentialsProvider)BUILDER.credentialsProvider;
        assertEquals("key2", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret2", credentials.resolveCredentials().secretAccessKey());
    }
}

