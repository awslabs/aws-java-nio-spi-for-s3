/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.util.Map;

public class CreateBucket {
    public static void main(String[] args) throws IOException {

        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: CreateBucket <bucket-uri>");
        }

        if (!args[0].startsWith("s3://")) {
            throw new IllegalArgumentException("Bucket URI must start with s3://");
        }

        System.out.println("Creating bucket " + args[0]);
        try (var fs = FileSystems.newFileSystem(URI.create(args[0]),
                Map.of("locationConstraint", "us-west-2"))) {
            System.out.println(fs.toString());
        } catch (FileSystemAlreadyExistsException e) {
            System.err.printf("Bucket already exists: %s\n", e.getMessage());
        }
    }
}
