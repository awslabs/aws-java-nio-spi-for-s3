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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@SuppressWarnings("unchecked")
//@ExtendWith(MockitoExtension.class)
public class S3ClientStoreEndpointTest {


    @Test
    public void testAsyncClientWithProvidedEndpointAndCredentials() throws Exception {
        final String BUCKET1 = "key1:secret1@endpoint1.io/bucket1";
        final String BUCKET2 = "key2:secret2@endpoint2.io:8080/bucket2";

        final FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();

        S3ClientStore cs = new S3ClientStore();
        cs.asyncClientBuilder = BUILDER;

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "aws-east-1");
            cs.generateAsyncClient(BUCKET1);
        });

        assertEquals(URI.create("https://endpoint1.io"), BUILDER.endpointOverride);
        assertTrue(BUILDER.credentialsProvider instanceof AwsCredentialsProvider);
        AwsCredentialsProvider credentials = (AwsCredentialsProvider)BUILDER.credentialsProvider;
        assertEquals("key1", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret1", credentials.resolveCredentials().secretAccessKey());
    }
}

