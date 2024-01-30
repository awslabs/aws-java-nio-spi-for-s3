/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

@SuppressWarnings("CheckStyle")
public class ListPrefix {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Provide an s3 prefix to list.");
            //logger.error("Provide an s3 prefix to list.");
            System.exit(1);
        }

        var s3Path = Paths.get(URI.create(args[0]));

        // if the prefix doesn't exist you won't get any listings
        if (!Files.exists(s3Path)) {
            System.err.printf("the prefix %s doesn't exist, you won't get any listings%n", s3Path);
        }

        //if it's not a directory you won't get any listings.
        if (!Files.isDirectory(s3Path)) {
            System.err.printf("the path %s is not a directory, you won't get any listings%n", s3Path);
        }

        try (var listed = Files.list(s3Path)) {
            listed.forEach(System.out::println);
        }
    }
}
