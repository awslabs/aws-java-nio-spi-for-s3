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

        // Delete the file first. As of 3.0, Files.delete follows the NIO contract: deleting a
        // non-empty directory throws DirectoryNotEmptyException, so the prefix must be emptied
        // before it can be removed.
        Files.delete(filePath);
        System.out.println("Deleted: " + filePath);

        // Now that the prefix is empty its directory marker can be deleted.
        Files.delete(pathCreated);
        System.out.println("Deleted: " + pathCreated);
    }
}
