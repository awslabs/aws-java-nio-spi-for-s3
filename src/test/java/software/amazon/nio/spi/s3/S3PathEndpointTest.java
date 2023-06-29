/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3;

import java.net.URI;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class S3PathEndpointTest {
    @Test
    public void stripCredentialsAndEndpoint() {
        S3FileSystem fs = new S3FileSystem(URI.create("s3://key:secret@somewhere.com:1010/bucket"), new S3FileSystemProvider());

        assertEquals("afile.txt", S3Path.getPath(fs, "afile.txt").toString());
        assertEquals("/afile.txt", S3Path.getPath(fs, "s3://bucket/afile.txt").toString());
        assertEquals("/afile.txt", S3Path.getPath(fs, "s3://somewhere.com:1010/bucket/afile.txt").toString());
        assertEquals("/afile.txt", S3Path.getPath(fs, "s3://key:secret@somewhere.com:1010/bucket/afile.txt").toString());
    }

    @Test
    public void stripCredentialsAndEndpointNIO() {
        assertEquals("/afile.txt", Paths.get(URI.create("s3://bucket/afile.txt")).toString());
        assertEquals("/afile.txt", Paths.get(URI.create("s3://somewhere.com:1010/bucket/afile.txt")).toString());
        assertEquals("/afile.txt", Paths.get(URI.create("s3://key:secret@somewhere.com:1010/bucket/afile.txt")).toString());
    }
}
