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

public class ListPrefix {

    private static final Logger logger = LoggerFactory.getLogger(ListPrefix.class);

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            logger.error("Provide an s3 prefix to list.");
            System.exit(1);
        }

        var s3Path = Paths.get(URI.create(args[0]));

        // if the prefix doesn't exist you won't get any listings
        if (!Files.exists(s3Path)) {
            logger.error("the prefix {} doesn't exist, you won't get any listings", s3Path);
        }

        //if it's not a directory you won't get any listings.
        if (!Files.isDirectory(s3Path)) {
            logger.error("the path {} is not a directory, you won't get any listings", s3Path);
        }

        try (var listed = Files.list(s3Path)) {
            listed.forEach((p) -> logger.info(p.toString()));
        }
    }
}
