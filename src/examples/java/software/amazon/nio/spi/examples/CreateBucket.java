/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.util.Map;

public class CreateBucket {
    public static void main(String[] args) throws IOException {
        try (var fs = FileSystems.newFileSystem(URI.create(args[0]),
                Map.of("locationConstraint", "us-east-1"))) {
            System.out.println(fs.toString());
        }
    }
}
