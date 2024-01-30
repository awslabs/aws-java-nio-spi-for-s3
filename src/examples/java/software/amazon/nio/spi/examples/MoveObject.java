/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Demonstrates a move operation using the `Files` class
 */
@SuppressWarnings("CheckStyle")
public class MoveObject {


    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            System.err.println("Usage: java MoveObject <source> <destination>");
            System.exit(1);
        }

        System.err.printf("Moving %s to %s%n", args[0], args[1]);

        URI source = URI.create(args[0]);
        URI destination = URI.create(args[1]);
        Files.move(Path.of(source), Path.of(destination));
    }
}
