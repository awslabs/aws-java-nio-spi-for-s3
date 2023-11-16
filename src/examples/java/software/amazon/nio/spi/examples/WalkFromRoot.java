/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;


import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WalkFromRoot {

    private static final Logger logger = LoggerFactory.getLogger(WalkFromRoot.class);

    /**
     * Walks a bucket from the root listing all directories and files. Internally uses the {@code S3FileSystemProvider}'s
     * {@code newDirectoryStream()} method which uses asynchronous paginated calls to S3.
     *
     * @param args provide a bucket name to walk
     * @throws IOException if a communication problem happens with the S3 service.
     */
    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            logger.error("Provide a bucket name to walk");
            System.exit(1);
        }

        var bucketName = args[0];
        if (!bucketName.startsWith("s3:") && !bucketName.startsWith("s3x:")) {
            bucketName = "s3://" + bucketName;
        }


        var root = Paths.get(URI.create(bucketName));
        logger.info("root.getClass() = {}", root.getClass());

        var s3 = root.getFileSystem();

        for (var rootDir : s3.getRootDirectories()) {
            try (var pathStream = Files.walk(rootDir)) {
                pathStream.forEach((p) -> logger.info(p.toString()));
            }
        }

    }
}
