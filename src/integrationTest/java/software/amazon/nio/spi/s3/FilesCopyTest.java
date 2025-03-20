/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.*;
import static software.amazon.nio.spi.s3.Containers.*;

@DisplayName("Files$copy should load file contents from localstack")
public class FilesCopyTest {
    @TempDir
    Path tempDir;

    @Test
    @DisplayName("when doing copy of existing file")
    public void fileCopyShouldCopyFileWhenFileFound() throws IOException {
        Containers.createBucket("sink");

        final var path = putObject("sink", "files-copy.txt", "some content");

        var copiedFile = Files.copy(path, tempDir.resolve("sample-file-local.txt"));

        assertThat(copiedFile).hasContent("some content");
    }

    @Test
    @DisplayName("when copying a non-existent file a NoSuchFileException is thrown")
    void fileCopyShouldThrowNoSuchFileException() throws IOException {
        Containers.createBucket("some-bucket");

        var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/some-bucket/non-existent-file.txt"));

        assertThatThrownBy(() -> Files.copy(path, tempDir.resolve("target.txt")))
            .isInstanceOf(NoSuchFileException.class)
            .hasMessage(path.toString());
    }
}
