/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3x;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.nio.spi.s3.S3FileSystem;
import software.amazon.nio.spi.s3.S3FileSystemProvider;
import software.amazon.nio.spi.s3.S3Path;

/**
 *
 */
public class S3PathTest {
    @BeforeEach
    public void before() throws Exception {
        Field f = S3FileSystemProvider.class.getDeclaredField("cache");
        f.setAccessible(true);
        Map cache = (Map)f.get(null);
        cache.clear();
    }

    @Test
    public void stripCredentialsAndEndpointNIO() {
        then(Paths.get(URI.create("s3x://somewhere.com:1010/bucket/afile.txt")).toString()).isEqualTo("/afile.txt");
        then(Paths.get(URI.create("s3x://key:secret@somewhere.com:1010/bucket/afile.txt")).toString()).isEqualTo("/afile.txt");
    }

    @Test
    public void stripCredentialsAndEndpoint() throws Exception {
        S3XFileSystemProvider provider = new S3XFileSystemProvider();

        S3FileSystem fs = provider.newFileSystem(URI.create("s3x://key:secret@somewhere.com:1010/bucket"));
        then(S3Path.getPath(fs, "s3x://somewhere.com:1010/bucket/afile.txt").toString()).isEqualTo("/afile.txt");
        then(S3Path.getPath(fs, "s3x://key:secret@somewhere.com:1010/bucket/afile.txt").toString()).isEqualTo("/afile.txt");
        provider.closeFileSystem(fs);
    }
}
