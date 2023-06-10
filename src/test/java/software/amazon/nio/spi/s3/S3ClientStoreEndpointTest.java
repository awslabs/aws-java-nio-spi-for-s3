/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.net.URI;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import static software.amazon.awssdk.awscore.client.config.AwsClientOption.CREDENTIALS_PROVIDER;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class S3ClientStoreEndpointTest extends TestCase {

    @Rule
    public final ProvideSystemProperty AWS_PROPERTIES
        = new ProvideSystemProperty("aws.region", "aws-east-1");


    @Test
    public void testAsyncClientWithProvidedEndpointAndCredentials() throws Exception {
        final String BUCKET1 = "key1:secret1@endpoint1.io/bucket1";
        final String BUCKET2 = "key2:secret2@endpoint2.io:8080/bucket2";

        final FakeAsyyncS3ClientBuilder BUILDER = new FakeAsyyncS3ClientBuilder();

        S3ClientStore cs = new S3ClientStore();
        cs.asyncClientBuilder = BUILDER;

        cs.generateAsyncClient(BUCKET1);

        assertEquals(URI.create("https://endpoint1.io"), BUILDER.endpointOverride);
        assertTrue(BUILDER.credentialsProvider instanceof AwsCredentialsProvider);
        AwsCredentialsProvider credentials = (AwsCredentialsProvider)BUILDER.credentialsProvider;
        assertEquals("key1", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret1", credentials.resolveCredentials().secretAccessKey());
    }
}

