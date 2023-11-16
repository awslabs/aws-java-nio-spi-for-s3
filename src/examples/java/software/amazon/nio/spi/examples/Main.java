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

/**
 * Demo class that will read text from S3 URIs that you provide on the command line. Your standard AWS credential chain
 * is used for permissions.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Demo main method
     *
     * @param args one or more S3 URIs to read
     * @throws IOException if something goes wrong
     */
    public static void main(String[] args) throws IOException {

        if (args.length == 0) {
            logger.error("Provide one or more S3 URIs to read from.");
            System.exit(1);
        }

        for (var pathString : args) {

            // if the URI starts with "s3:" then Paths will use the spi to handle the paths and reading
            final var path = Paths.get(URI.create(pathString));

            // proves that the correct path type is being used
            assert path.getClass().getName().contains("S3Path");

            logger.info("*** READING FROM {} ***", path.toUri());
            Files.readAllLines(path)
                .forEach(logger::info);
            logger.info("*** FINISHED READING OBJECT ***");
        }
    }
}
