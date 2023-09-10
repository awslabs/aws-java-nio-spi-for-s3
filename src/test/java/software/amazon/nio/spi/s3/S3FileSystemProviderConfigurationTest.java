/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_ACCESS_KEY_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_REGION_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_SECRET_ACCESS_KEY_PROPERTY;

public class S3FileSystemProviderConfigurationTest {

    final FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();


    @Test
    public void setEndpointProtocolThroughConfiguration() throws Exception {
        S3NioSpiConfiguration env = new S3NioSpiConfiguration();
        env.put(AWS_REGION_PROPERTY, "us-west-1");

        S3FileSystemProvider p = new S3FileSystemProvider();

        S3FileSystem fs = p.newFileSystem(URI.create("s3://some.where.com:1010/bucket"), env);
        fs.clientProvider.asyncClientBuilder = BUILDER;
        fs.client(); fs.close();

        then(fs.bucketName()).isEqualTo("bucket");
        then(fs.configuration().getEndpoint()).isEqualTo("some.where.com:1010");
        then(BUILDER.endpointOverride.toString()).isEqualTo("https://some.where.com:1010");

        env.withEndpointProtocol("http");

        fs = p.newFileSystem(URI.create("s3://any.where.com:2020/foo"), env);
        fs.clientProvider.asyncClientBuilder = BUILDER;
        fs.client(); fs.close();

        then(fs.bucketName()).isEqualTo("foo");
        then(fs.configuration().getEndpoint()).isEqualTo("any.where.com:2020");
        then(BUILDER.endpointOverride.toString()).isEqualTo("http://any.where.com:2020");
    }

    @Test
    public void setCredentialsThroughMap() throws Exception {
        S3FileSystemProvider p = new S3FileSystemProvider();
        Map<String, String> env = new HashMap<>();
        env.put(AWS_ACCESS_KEY_PROPERTY, "envkey");
        env.put(AWS_SECRET_ACCESS_KEY_PROPERTY, "envsecret");

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");
            System.setProperty(AWS_ACCESS_KEY_PROPERTY, "systemkey");
            System.setProperty(AWS_SECRET_ACCESS_KEY_PROPERTY, "systemsecret");

            S3FileSystem fs = p.newFileSystem(URI.create("s3://some.where.com:1010/bucket"), env);
            fs.clientProvider.asyncClientBuilder = BUILDER;
            fs.client(); fs.close();

            assertEquals("bucket", fs.bucketName());
            assertEquals("some.where.com:1010", fs.configuration().getEndpoint());
            assertEquals("https://some.where.com:1010", BUILDER.endpointOverride.toString());
            assertEquals("envkey", BUILDER.credentialsProvider.resolveCredentials().accessKeyId());
            assertEquals("envsecret", BUILDER.credentialsProvider.resolveCredentials().secretAccessKey());
        });
    }

    @Test
    public void setCredentialsThroughURI() throws Exception {
        S3FileSystemProvider p = new S3FileSystemProvider();
        Map<String, String> env = new HashMap<>();
        env.put(AWS_ACCESS_KEY_PROPERTY, "envkey");
        env.put(AWS_SECRET_ACCESS_KEY_PROPERTY, "envsecret");

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");

            S3FileSystem fs = p.newFileSystem(URI.create("s3://urikey:urisecret@some.where.com:1010/bucket"), env);
            fs.clientProvider.asyncClientBuilder = BUILDER;
            fs.client(); fs.close();

            assertEquals("bucket", fs.bucketName());
            assertEquals("some.where.com:1010", fs.configuration().getEndpoint());
            assertEquals("https://some.where.com:1010", BUILDER.endpointOverride.toString());
            assertEquals("urikey", BUILDER.credentialsProvider.resolveCredentials().accessKeyId());
            assertEquals("urisecret", BUILDER.credentialsProvider.resolveCredentials().secretAccessKey());
        });
    }


}
