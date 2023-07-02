/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.net.URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class S3URITest {

    @Test
    public void constructorWithOnlyBucket() {
        S3URI u = new S3URI("mybucket");
        assertEquals("mybucket", u.bucket());
        assertNull(u.endpoint()); assertNull(u.accessKey());
        assertNull(u.secretAccessKey()); assertNull(u.path());

        try {
            new S3URI(null);
            fail("sanity check missing");
        } catch (NullPointerException x) {
            assertEquals("bucket can not be null", x.getMessage());
        }
    }

    @Test
    public void constructorWithOnlyAllElements() {
        givenUriThen(
            new S3URI("yourbucket1", "endpoint1", "key1", "secret1", "/path1"),
            "yourbucket1", "endpoint1", "key1", "secret1", "/path1"
        );

        givenUriThen(
            new S3URI("yourbucket2", "endpoint2", "key2", "secret2", "/path2"),
            "yourbucket2", "endpoint2", "key2", "secret2", "/path2"
        );

        givenUriThen(
            new S3URI("bucket", null, "key", "secret", "/path"),
            "bucket", null, "key", "secret", "/path"
        );

        givenUriThen(
            new S3URI("bucket", "endpoint", null, "secret", "/path"),
            "bucket", "endpoint", null, null, "/path"
        );

        try {
            new S3URI("bucket", "endpoint", "key2", null, "/path");
            fail("sanity check missing");
        } catch (NullPointerException x) {
            assertEquals("secretAccessKey can not be null if accessKey is not null", x.getMessage());
        }


        givenUriThen(
            new S3URI("bucket", "endpoint", "key", "secret", null),
            "bucket", "endpoint", "key", "secret", null
        );

        givenUriThen(
            new S3URI("bucket", "endpoint", null, "secret", "/"),
            "bucket", "endpoint", null, null, "/"
        );
    }

    @Test
    public void credentials() {
        S3URI u = new S3URI("bucket");
        assertNull(u.credentials());

        u = new S3URI("bucket", "endpoint", null, "secret", "/path");
        assertNull(u.credentials());

        u = new S3URI("bucket", "endpoint", "key1", "secret1", "/path");
        assertNotNull(u.credentials());
        assertEquals("key1", u.credentials().accessKeyId());
        assertEquals("secret1", u.credentials().secretAccessKey());

        u = new S3URI("bucket", "endpoint", "key2", "secret2", "/path");
        assertEquals("key2", u.credentials().accessKeyId());
        assertEquals("secret2", u.credentials().secretAccessKey());
    }

    @Test
    public void fileSystemKey() {
        S3URI u = new S3URI("mybucket");
        assertEquals("mybucket", u.fileSystemKey());

        u = new S3URI("yourbucket", "endpoint", "key2", "secret2", "/path");
        assertEquals("endpoint/yourbucket", u.fileSystemKey());
    }

    @Test
    public void of() throws Exception {
        try {
            S3URI.of(null);
            fail("sanity check missing");
        } catch (NullPointerException x) {
            assertEquals("uri can not be null", x.getMessage());
        }

        try {
            S3URI.of(new URI(null, null, "bucket"));
            fail("invalid uri '%s', please provide an uri as s3://[key:secret@][host:port]/bucket");
        } catch (IllegalArgumentException x) {
            assertEquals("invalid uri '#bucket', please provide an uri as s3://[key:secret@][host:port]/bucket", x.getMessage());
        }

        try {
            S3URI.of(new URI("s3", null, "/bucket", null, null));
            fail("invalid uri '%s', please provide an uri as s3://[key:secret@][host:port]/bucket");
        } catch (IllegalArgumentException x) {
            assertEquals("invalid uri 's3:/bucket', please provide an uri as s3://[key:secret@][host:port]/bucket", x.getMessage());
        }

        S3URI u = S3URI.of(URI.create("s3://mybucket/myobjectkey"));
        assertEquals("mybucket", u.bucket()); assertEquals("/myobjectkey", u.path());
        assertNull(u.endpoint()); assertNull(u.accessKey()); assertNull(u.secretAccessKey());

        u = S3URI.of(URI.create("s3://yourendpoint:1010/yourbucket/yourobjectkey"));
        assertEquals("yourbucket", u.bucket()); assertEquals("yourendpoint:1010", u.endpoint());
        assertEquals("/yourobjectkey", u.path()); assertNull(u.accessKey()); assertNull(u.secretAccessKey());

        u = S3URI.of(URI.create("s3://ourkey:oursecret@ourendpoint:1010/ourbucket/ourobjectkey"));
        assertEquals("ourbucket", u.bucket()); assertEquals("ourendpoint:1010", u.endpoint());
        assertEquals("/ourobjectkey", u.path());
        assertEquals("ourkey", u.accessKey()); assertEquals("oursecret", u.secretAccessKey());
    }

    // --------------------------------------------------------- private methods

    private void givenUriThen(S3URI u, String bucket, String endpoint, String accessKey, String secretAccessKey, String path) {
        assertEquals(bucket, u.bucket()); assertEquals(endpoint, u.endpoint());
        assertEquals(accessKey, u.accessKey()); assertEquals(secretAccessKey, u.secretAccessKey());
        assertEquals(path, u.path());
    }

}
