/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public class CreatePrefix {
    public static void main(String[] args) throws IOException, URISyntaxException {
        var bucketName =  args[0];
        var prefix = args[1];

        var pathUri = new URI("s3://" + bucketName + "/" + prefix);

        // creates the directories (called a prefix in s3)
        var pathCreated = Files.createDirectories(Path.of(pathUri));
        System.out.println("Created:" + pathCreated);

        // writes a file to the prefix
        var filePath = pathCreated.resolve("test.txt");
        var written = Files.write(filePath, "This is some test text.".getBytes());
        System.out.println("Wrote to: " + written);

        // read the file content to stdout
        System.out.println("File content: " + Files.readString(filePath));

        // delete the file
        Files.delete(filePath);
        System.out.println("Deleted: " + filePath);

        // delete the prefix
        Files.delete(pathCreated);
        System.out.println("Deleted: " + pathCreated);
    }
}
