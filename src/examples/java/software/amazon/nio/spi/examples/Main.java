/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Demo class that will read text from S3 URIs that you provide on the command line. Your standard AWS credential chain
 * is used for permissions.
 */
@SuppressWarnings("CheckStyle")
public class Main {
    public static void main(String[] args) throws IOException {

        if (args.length == 0) {
            System.err.println("Provide one or more S3 URIs to read from.");
            System.exit(1);
        }

        for (var pathString : args) {

            // if the URI starts with "s3:" then Paths will use the spi to handle the paths and reading
            final var path = Paths.get(URI.create(pathString));

            // proves that the correct path type is being used
            assert path.getClass().getName().contains("S3Path");

            System.err.printf("*** READING FROM %s ***%n", path.toUri());
            Files.readAllLines(path)
                .forEach(System.out::println);
            System.err.println("*** FINISHED READING OBJECT ***");
        }
    }
}
