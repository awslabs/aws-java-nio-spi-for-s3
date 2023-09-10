/*
 * Copyright 2023 ste.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.nio.spi.s3.util;

import java.net.URI;
import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class S3FileSystemInfoTest {
    @Test
    public void construction() {
        S3FileSystemInfo info = new S3FileSystemInfo(URI.create("s3://abucket/something"));
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

        try {
            new S3FileSystemInfo(URI.create("s2://Wrong$bucket;name"));
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("Bucket name should not contain uppercase characters");
        }

        try {
            new S3FileSystemInfo(null);
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("uri can not be null");
        }
    }
}
