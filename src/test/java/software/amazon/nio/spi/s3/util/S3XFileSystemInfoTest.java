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
 * @author ste
 */
public class S3XFileSystemInfoTest {

    @Test
    public void construction() {
        S3XFileSystemInfo info = new S3XFileSystemInfo(URI.create("s3://myendpoint/mybucket/something"));
        then(info.key()).isEqualTo("myendpoint/mybucket");
        then(info.bucket()).isEqualTo("mybucket");
        then(info.endpoint()).isEqualTo("myendpoint");
        then(info.accessKey()).isNull();
        then(info.accessSecret()).isNull();

        info = new S3XFileSystemInfo(URI.create("s3://yourendpoint/yourbucket/somethingelse"));
        then(info.key()).isEqualTo("yourendpoint/yourbucket");
        then(info.bucket()).isEqualTo("yourbucket");
        then(info.endpoint()).isEqualTo("yourendpoint");
        then(info.accessKey()).isNull();
        then(info.accessSecret()).isNull();

        assertThatCode(() -> new S3XFileSystemInfo(URI.create("s2://myendpoint/Wrong$bucket;name")))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket name should not contain uppercase characters");

        assertThatCode(() -> new S3XFileSystemInfo(null))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("uri can not be null");
    }

    @Test
    public void user_info() {
        S3XFileSystemInfo info = new S3XFileSystemInfo(URI.create("s3://key@my.service.com:1111/mybucket"));
        then(info.key()).isEqualTo("key@my.service.com:1111/mybucket");
        then(info.bucket()).isEqualTo("mybucket");
        then(info.endpoint()).isEqualTo("my.service.com:1111");
        then(info.accessKey()).isEqualTo("key");
        then(info.accessSecret()).isNull();

        info = new S3XFileSystemInfo(URI.create("s3://key:secret@my.service.com/mybucket"));
        then(info.key()).isEqualTo("key@my.service.com/mybucket");
        then(info.bucket()).isEqualTo("mybucket");
        then(info.endpoint()).isEqualTo("my.service.com");
        then(info.accessKey()).isEqualTo("key");
        then(info.accessSecret()).isEqualTo("secret");
    }
}

