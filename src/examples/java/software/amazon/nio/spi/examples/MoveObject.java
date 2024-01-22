/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demonstrates a move operation using the `Files` class
 */
public class MoveObject {

    private static final Logger logger = LoggerFactory.getLogger(MoveObject.class.getName());

    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            logger.error("Usage: java MoveObject <source> <destination>");
            System.exit(1);
        }

        logger.info("Moving {} to {}", args[0], args[1]);

        URI source = URI.create(args[0]);
        URI destination = URI.create(args[1]);
        Files.move(Path.of(source), Path.of(destination));
    }
}
