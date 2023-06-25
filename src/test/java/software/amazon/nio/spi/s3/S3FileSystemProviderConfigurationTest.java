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
import org.junit.jupiter.api.Test;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_ACCESS_KEY_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_SECRET_ACCESS_KEY_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.S3_SPI_ENDPOINT_PROTOCOL_PROPERTY;

public class S3FileSystemProviderConfigurationTest {

    final FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();


    @Test
    public void setEndpointProtocolThroughEnvironment() throws Exception {
        Map<String, String> env = new HashMap<>();
        S3FileSystemProvider p = new S3FileSystemProvider();

        //
        // TODO: remove in favor of env.put("aws.region", "...");
        //
        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");

            S3FileSystem fs = p.newFileSystem(URI.create("s3://some.where.com:1010/bucket"), env);
            fs.clientProvider.asyncClientBuilder = BUILDER;
            fs.client(); fs.close();

            assertEquals("bucket", fs.bucketName());
            assertEquals("some.where.com:1010", fs.endpoint());
            assertEquals("https://some.where.com:1010", BUILDER.endpointOverride.toString());

            env.put(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, "http");

            fs = p.newFileSystem(URI.create("s3://any.where.com:2020/foo"), env);
            fs.clientProvider.asyncClientBuilder = BUILDER;
            fs.client(); fs.close();

            assertEquals("foo", fs.bucketName());
            assertEquals("any.where.com:2020", fs.endpoint());
            assertEquals("http://any.where.com:2020", BUILDER.endpointOverride.toString());
        });
    }

    @Test
    public void setEndpointProtocolThroughSystemProperties() throws Exception {
        S3FileSystemProvider p = new S3FileSystemProvider();

        //
        // TODO: remove in favor of env.put("aws.region", "...");
        //
        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");

            S3FileSystem fs = p.newFileSystem(URI.create("s3://some.where.com:1010/bucket"));
            fs.clientProvider.asyncClientBuilder = BUILDER;
            fs.client(); fs.close();

            assertEquals("bucket", fs.bucketName());
            assertEquals("some.where.com:1010", fs.endpoint());
            assertEquals("https://some.where.com:1010", BUILDER.endpointOverride.toString());

            System.setProperty(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, "http");

            fs = p.newFileSystem(URI.create("s3://any.where.com:2020/foo"));
            fs.clientProvider.asyncClientBuilder = BUILDER;
            fs.client(); fs.close();

            assertEquals("foo", fs.bucketName());
            assertEquals("any.where.com:2020", fs.endpoint());
            assertEquals("http://any.where.com:2020", BUILDER.endpointOverride.toString());
        });
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
            assertEquals("some.where.com:1010", fs.endpoint());
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
            assertEquals("some.where.com:1010", fs.endpoint());
            assertEquals("https://some.where.com:1010", BUILDER.endpointOverride.toString());
            assertEquals("urikey", BUILDER.credentialsProvider.resolveCredentials().accessKeyId());
            assertEquals("urisecret", BUILDER.credentialsProvider.resolveCredentials().secretAccessKey());
        });
    }


}
