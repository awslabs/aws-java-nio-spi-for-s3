/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("FileSystems$newFileSystem(URI, Map) should")
public class FileSystemsNewFSTest {

    @Test
    @DisplayName("create bucket when name is valid")
    public void createBucket() throws IOException {
        String bucketLocation = Containers.localStackConnectionEndpoint() + "/fresh-bucket";
        FileSystems.newFileSystem(
            URI.create(bucketLocation),
            Map.of("locationConstraint", "us-east-1")
        );
    }
}
