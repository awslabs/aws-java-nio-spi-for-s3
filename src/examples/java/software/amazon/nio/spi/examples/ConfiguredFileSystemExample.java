/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import software.amazon.nio.spi.s3.S3FileSystem;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 * Demonstrates supplying per-filesystem configuration (region, credentials, and a timeout) through
 * the standard {@link FileSystems#newFileSystem(URI, Map)} {@code env} map.
 *
 * <p>As of 3.0 the {@code env} map is merged into the file system's {@link S3NioSpiConfiguration}.
 * Keys are the same property names used for system properties / environment variables (see
 * {@link S3NioSpiConfiguration}); values may be {@code String}s or already-typed objects (for
 * example an {@code AwsCredentials} or {@code AwsCredentialsProvider} under
 * {@code s3.spi.credentials} / {@code s3.spi.credentials.provider}). The env map takes precedence
 * over values parsed from the URI and over system properties / environment variables.
 *
 * <p>Usage: {@code ConfiguredFileSystemExample s3://my-bucket eu-central-1 [accessKey secretKey]}
 */
@SuppressWarnings("CheckStyle")
public class ConfiguredFileSystemExample {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println(
                "Usage: ConfiguredFileSystemExample <s3-uri> <region> [<accessKey> <secretKey>]");
            System.exit(1);
        }

        var uri = URI.create(args[0]);
        var region = args[1];

        Map<String, Object> env = new HashMap<>();
        env.put(S3NioSpiConfiguration.AWS_REGION_PROPERTY, region);
        // Use a short "low" timeout (in minutes) for this file system.
        env.put(S3NioSpiConfiguration.S3_SPI_TIMEOUT_LOW_PROPERTY, "2");

        // Optionally supply explicit credentials (otherwise the AWS default credential provider
        // chain is used). Here we pass them as strings; an AwsCredentials / AwsCredentialsProvider
        // object under s3.spi.credentials / s3.spi.credentials.provider works too.
        if (args.length >= 4) {
            env.put(S3NioSpiConfiguration.AWS_ACCESS_KEY_PROPERTY, args[2]);
            env.put(S3NioSpiConfiguration.AWS_SECRET_ACCESS_KEY_PROPERTY, args[3]);
        }

        // newFileSystem provisions the bucket if needed and binds the configuration above. A second
        // newFileSystem call for the same bucket in this JVM would throw
        // FileSystemAlreadyExistsException, per the java.nio.file contract.
        try (FileSystem fs = FileSystems.newFileSystem(uri, env)) {
            var configuration = ((S3FileSystem) fs).getConfiguration();
            System.out.println("Configured region : " + configuration.getRegion());
            System.out.println("Configured timeout: " + configuration.getTimeoutLow() + " minute(s)");

            // List the top-level entries of the bucket to prove the configured file system works.
            var root = fs.getPath("/");
            try (var entries = Files.newDirectoryStream(root)) {
                System.out.println("Top-level entries:");
                entries.forEach(p -> System.out.println("  " + p));
            }
        }
    }
}
