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
public class MoveDirectory {


    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            System.err.println("Usage: java MoveObject <sourceDir> <destinationDir>");
            System.exit(1);
        }

        final URI source = URI.create(args[0]);
        final URI destination = URI.create(args[1]);

        final Path destinationPath = Path.of(destination);
        final Path sourcePath = Path.of(source);

        assert Files.isDirectory(sourcePath);
        assert Files.isDirectory(destinationPath);

        System.err.println("Source path: " + sourcePath);
        System.err.println("Destination path: " + destinationPath);

        System.err.printf("Moving %s to %s%n", args[0], args[1]);

        Path movedTo = Files.move(sourcePath, destinationPath);

        System.err.printf("Moved %s to %s%n", sourcePath, movedTo);

        // move it back
        Files.move(movedTo, sourcePath);
        System.err.printf("Moved %s back to %s%n", movedTo, sourcePath);
    }
}
