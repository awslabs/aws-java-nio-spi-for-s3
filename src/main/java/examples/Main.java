/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package examples;

import software.amazon.nio.spi.s3.S3Path;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Demo class that will read text from S3 URIs that you provide on the command line. Your standard AWS credential chain
 * is used for permissions.
 */
public class Main {
    /**
     * Demo main method
     * @param args one or more S3 URIs to read
     * @throws IOException if something goes wrong
     */
    public static void main(String[] args) throws IOException {

        if(args.length == 0){
            System.out.println("Provide one or more S3 URIs to read from.");
            System.exit(1);
        }

        for (String pathString : args) {

            // if the URI starts with "s3:" then Paths will use the spi to handle the paths and reading
            final Path path = Paths.get(URI.create(pathString));

            // proves that the correct path type is being used
            assert path instanceof S3Path;

            System.out.println("*** READING FROM "+path.toUri()+" ***");
            Files.readAllLines(path)
                    .forEach(System.out::println);
            System.out.println("*** FINISHED READING OBJECT ***\n");
        }
    }
}
