/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import org.junit.jupiter.api.Test;

class S3BasicFileAttributeViewTest {
    final String uriString = "s3://mybucket";
    final S3FileSystemProvider provider = new S3FileSystemProvider();

    S3FileSystem fileSystem = provider.getFileSystem(URI.create(uriString), true);
    S3Path path = S3Path.getPath(fileSystem, uriString);
    S3BasicFileAttributeView view = new S3BasicFileAttributeView(path);

    @Test
    void setTimes() {
        assertDoesNotThrow(() -> view.setTimes(null, null, null));
    }

    @Test
    void name() {
        assertEquals("s3", view.name());
    }
}