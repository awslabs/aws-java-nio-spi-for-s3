/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.nio.spi.s3.Containers.localStackConnectionEndpoint;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Files$newDirectoryStream()")
public class FilesDirectoryStreamTest {

    @Nested
    @DisplayName("when bucket does not exist")
    class DirectoryDoesNotExist {

        @DisplayName("should throw")
        @Test
        @SuppressWarnings("resource")
        public void whenBucketNotFound() {
            final var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/does-not-exist/some-directory"));
            assertThatThrownBy(() -> Files.newDirectoryStream(path, p -> true))
                .isInstanceOf(IOException.class);
        }
    }

    @Nested
    @DisplayName("when bucket exists")
    class DirectoryExists {

        @DisplayName("and is empty, list should be empty")
        @Test
        public void listShouldBeEmpty() throws IOException {
            Containers.createBucket("new-directory-stream");
            final var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/new-directory-stream/"));

            try(var stream = Files.newDirectoryStream(path, p -> true)) {
                assertThat(stream).isEmpty();
            }
        }
    }

}
