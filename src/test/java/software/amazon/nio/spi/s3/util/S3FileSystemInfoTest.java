/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import java.net.URI;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.BDDAssertions.then;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class S3FileSystemInfoTest {
    @Test
    public void construction() {
        var info = new S3FileSystemInfo(URI.create("s3://abucket/something"));
        then(info.key()).isEqualTo("abucket");
        then(info.bucket()).isEqualTo("abucket");
        then(info.endpoint()).isNull();
        then(info.accessKey()).isNull();
        then(info.accessSecret()).isNull();

        info = new S3FileSystemInfo(URI.create("s3://anotherbucket/something/else"));
        then(info.key()).isEqualTo("anotherbucket");
        then(info.bucket()).isEqualTo("anotherbucket");
        then(info.endpoint()).isNull();
        then(info.accessKey()).isNull();
        then(info.accessSecret()).isNull();

        assertThatCode(() -> new S3FileSystemInfo(URI.create("s2://Wrong$bucket;name")))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket name should not contain uppercase characters");

        assertThatCode(() -> new S3FileSystemInfo(null))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("uri can not be null");
    }
}
