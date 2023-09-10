/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class S3PathEndpointTest {
    @BeforeEach
    public void before() throws Exception {
        Field f = S3FileSystemProvider.class.getDeclaredField("cache");
        f.setAccessible(true);
        Map cache = (Map)f.get(null);
        cache.clear();
    }

    @Test
    public void stripCredentialsAndEndpointNIO() {
        assertEquals("/afile.txt", Paths.get(URI.create("s3://bucket/afile.txt")).toString());
        assertEquals("/afile.txt", Paths.get(URI.create("s3://somewhere.com:1010/bucket/afile.txt")).toString());
        assertEquals("/afile.txt", Paths.get(URI.create("s3://key:secret@somewhere.com:1010/bucket/afile.txt")).toString());
    }

    @Test
    public void stripCredentialsAndEndpoint() throws Exception {
        S3FileSystemProvider provider = new S3FileSystemProvider();

        S3FileSystem fs = provider.newFileSystem(URI.create("s3://key:secret@somewhere.com:1010/bucket"));
        assertEquals("/afile.txt", S3Path.getPath(fs, "s3://bucket/afile.txt").toString());
        assertEquals("/afile.txt", S3Path.getPath(fs, "s3://somewhere.com:1010/bucket/afile.txt").toString());
        assertEquals("/afile.txt", S3Path.getPath(fs, "s3://key:secret@somewhere.com:1010/bucket/afile.txt").toString());
        provider.closeFileSystem(fs);
    }
}
