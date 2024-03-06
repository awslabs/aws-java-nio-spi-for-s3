/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.nio.spi.s3.Containers.localStackConnectionEndpoint;
import static software.amazon.nio.spi.s3.Containers.putObject;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@DisplayName("Files$write* should write file contents on s3 service")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesWriteTest {

    String bucketName;

    @BeforeEach
    public void createBucket() {
        bucketName = "write-bucket"+ System.currentTimeMillis();
        Containers.createBucket(bucketName);
    }

    @Test
    @DisplayName("append to an existing file")
    public void writeToExistingFile() throws IOException {
        final var path = putObject(bucketName, "existing-file.txt", "some content");

        Files.writeString(path, " more content", StandardOpenOption.APPEND);

        assertThat(path).hasContent("some content more content");
    }

    @Test
    @DisplayName("to a new file")
    public void writeToNewFile() throws IOException {
        final var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/"+bucketName+"/new-file.txt"));

        Files.writeString(path, "content of new file", StandardOpenOption.CREATE_NEW);

        assertThat(path).hasContent("content of new file");
    }

}
