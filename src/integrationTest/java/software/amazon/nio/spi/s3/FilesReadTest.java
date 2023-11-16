/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.BDDAssertions.then;
import static software.amazon.nio.spi.s3.Containers.putObject;

@DisplayName("Files$read* should load file contents from localstack")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesReadTest {
    private Path path;

    @BeforeAll
    public void createBucketAndFile(){
        Containers.createBucket("sink");
        path = putObject("sink", "files-read.txt", "some content");
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
