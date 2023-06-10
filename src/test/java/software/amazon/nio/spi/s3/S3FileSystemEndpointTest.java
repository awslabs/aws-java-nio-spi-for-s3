/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URI;

import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import static software.amazon.awssdk.core.client.config.SdkClientOption.ENDPOINT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.ENDPOINT_OVERRIDDEN;
import static software.amazon.awssdk.awscore.client.config.AwsClientOption.CREDENTIALS_PROVIDER;
import software.amazon.awssdk.services.s3.S3Client;

@RunWith(MockitoJUnitRunner.class)
public class S3FileSystemEndpointTest {
    S3FileSystemProvider provider;

    //
    // For non AWS S3 buckets, backet's region is not discovered runtime and it
    // must be provided in the AWS profile
    //
    @Rule
    public final ProvideSystemProperty AWS_PROPERTIES
        = new ProvideSystemProperty("aws.region", "aws-east-1");
    
    @Before
    public void init() {
        this.provider = new S3FileSystemProvider();
    }

    @Test
    public void clientWithProvidedEndpoint() throws Exception {
        final String URI1 = "s3://endpoint1.io/bucket/resource";
        final String URI2 = "s3://endpoint2.io:8080/bucket/resource";

        S3FileSystem fs = new S3FileSystem(URI.create(URI1), provider);
        S3Client client = fs.client();

        Field f = client.getClass().getDeclaredField("clientConfiguration");
        f.setAccessible(true);
        SdkClientConfiguration sdkConf = (SdkClientConfiguration)f.get(client);

        assertTrue(sdkConf.option(ENDPOINT_OVERRIDDEN));
        assertEquals(URI.create("https://endpoint1.io"), sdkConf.option(ENDPOINT));

        sdkConf = (SdkClientConfiguration)f.get(new S3FileSystem(URI.create(URI2), provider).client());
        assertTrue(sdkConf.option(ENDPOINT_OVERRIDDEN));
        assertEquals(URI.create("https://endpoint2.io:8080"), sdkConf.option(ENDPOINT));
    }

    @Test
    public void clientWithProvidedEndpointAndCredentials() throws Exception {
        final String URI1 = "s3://key1:secret1@endpoint1.io/bucket/resource";
        final String URI2 = "s3://key2:secret2@endpoint2.io:8080/bucket/resource";

        S3FileSystem fs = new S3FileSystem(URI.create(URI1), provider);
        S3Client client = fs.client();

        Field f = client.getClass().getDeclaredField("clientConfiguration");
        f.setAccessible(true);
        SdkClientConfiguration sdkConf = (SdkClientConfiguration)f.get(client);

        assertEquals(URI.create("https://endpoint1.io"), sdkConf.option(ENDPOINT));
        assertFalse(sdkConf.option(CREDENTIALS_PROVIDER) instanceof DefaultCredentialsProvider);
        AwsCredentialsProvider credentials = (AwsCredentialsProvider)sdkConf.option(CREDENTIALS_PROVIDER);
        assertEquals("key1", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret1", credentials.resolveCredentials().secretAccessKey());


        sdkConf = (SdkClientConfiguration)f.get(new S3FileSystem(URI.create(URI2), provider).client());
        assertEquals(URI.create("https://endpoint2.io:8080"), sdkConf.option(ENDPOINT));
        credentials = (AwsCredentialsProvider)sdkConf.option(CREDENTIALS_PROVIDER);
        assertEquals("key2", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret2", credentials.resolveCredentials().secretAccessKey());
    }

}