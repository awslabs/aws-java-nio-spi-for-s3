/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import org.junit.jupiter.api.Test;

class S3FileStoreTest {

    private final S3FileStore store = new S3FileStore("mybucket");

    @Test
    void nameIsBucketName() {
        assertEquals("mybucket", store.name());
    }

    @Test
    void typeIsS3() {
        assertEquals("s3", store.type());
    }

    @Test
    void isNotReadOnly() {
        assertFalse(store.isReadOnly());
    }

    @Test
    void spaceIsUnbounded() throws Exception {
        assertEquals(Long.MAX_VALUE, store.getTotalSpace());
        assertEquals(Long.MAX_VALUE, store.getUsableSpace());
        assertEquals(Long.MAX_VALUE, store.getUnallocatedSpace());
    }

    @Test
    void supportsBasicViewOnly() {
        assertTrue(store.supportsFileAttributeView(BasicFileAttributeView.class));
        assertFalse(store.supportsFileAttributeView(PosixFileAttributeView.class));
        assertFalse(store.supportsFileAttributeView((Class<? extends FileAttributeView>) null));

        assertTrue(store.supportsFileAttributeView("basic"));
        assertFalse(store.supportsFileAttributeView("posix"));
    }

    @Test
    void hasNoFileStoreAttributeView() {
        assertNull(store.getFileStoreAttributeView(FileStoreAttributeView.class));
    }

    @Test
    void getAttributeIsUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> store.getAttribute("totalSpace"));
    }

    @Test
    void equalsAndHashCodeBasedOnBucket() {
        assertEquals(store, new S3FileStore("mybucket"));
        assertEquals(store.hashCode(), new S3FileStore("mybucket").hashCode());
        assertNotEquals(store, new S3FileStore("otherbucket"));
        assertNotEquals(store, null);
        assertNotEquals(store, "mybucket");
        assertEquals(store, store);
    }

    @Test
    void toStringContainsBucket() {
        assertEquals("s3://mybucket", store.toString());
    }

    @Test
    void nullBucketRejected() {
        assertThrows(NullPointerException.class, () -> new S3FileStore(null));
    }
}
