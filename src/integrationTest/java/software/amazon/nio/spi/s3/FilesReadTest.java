/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.BDDAssertions.then;
import static software.amazon.nio.spi.s3.Containers.putObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@DisplayName("Files$read* should load file contents from localstack")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesReadTest {
    private Path path;

    @BeforeEach
    public void createBucketAndFile(){
        String containerName = "sink"+ System.currentTimeMillis();
        Containers.createBucket(containerName);
        path = putObject(containerName, "files-read.txt", "some content");
    }

    @Test
    @DisplayName("when doing readAllBytes from existing file in s3")
    public void fileReadAllBytesShouldReturnFileContentsWhenFileFound() throws IOException {
        then(Files.readAllBytes(path)).isEqualTo("some content".getBytes());
    }

    @Test
    @DisplayName("when doing readAllLines from existing file in s3")
    public void fileReadAllLinesShouldReturnFileContentWhenFileFound() throws IOException {
        then(String.join("", Files.readAllLines(path))).isEqualTo("some content");
    }

}
