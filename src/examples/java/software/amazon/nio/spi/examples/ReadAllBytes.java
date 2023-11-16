/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadAllBytes {

    private static final Logger logger = LoggerFactory.getLogger(ReadAllBytes.class);

    public static void main(String[] args) throws IOException {
        var filePath = Paths.get(URI.create(args[0]));

        final var bytes = Files.readAllBytes(filePath);
        // assumes this is a text file
        final var data = new String(bytes, StandardCharsets.UTF_8);
        logger.info(data);
    }
}
