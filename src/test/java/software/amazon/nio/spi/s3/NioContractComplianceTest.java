/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 * Regression tests that pin the NIO contract fixes described in
 * {@code docs/nio-contract-compliance-audit.md}. These are grouped here (rather than scattered
 * across the per-class test suites) so the specific contract guarantees are easy to find and hard
 * to accidentally regress.
 */
public class NioContractComplianceTest {

    private S3FileSystem newFileSystem(String bucket) {
        return new S3FileSystem(new S3FileSystemProvider(),
            new S3NioSpiConfiguration().withBucketName(bucket));
    }

    // ----- #1 PosixLikePathRepresentation(char[]) / relativize returns a usable string -----

    @Test
    void relativizeProducesNonNullBackedPath() {
        var fs = newFileSystem("bucket");
        var from = fs.getPath("/a/b/c/d/");
        var to = fs.getPath("/a/b/");

        var relative = from.relativize(to);
        // Before the fix this produced a Path whose toString() was null.
        assertNotNull(relative.toString());
        assertEquals("../..", relative.toString());
        assertFalse(relative.isAbsolute());
    }

    @Test
    void relativizeMatchesJdkExample() {
        var fs = newFileSystem("bucket");
        // The documented JDK example: "/a/b".relativize("/a/b/c/d") == "c/d".
        assertEquals("c/d", fs.getPath("/a/b").relativize(fs.getPath("/a/b/c/d")).toString());
    }

    // ----- #2 getParent stays absolute and terminates at root (#772) -----

    @Test
    void getParentChainStaysAbsoluteAndTerminates() {
        var fs = newFileSystem("bucket");
        var p = fs.getPath("/aa/bb/cc/");
        var chain = new ArrayList<String>();
        while (p != null) {
            chain.add(p.toString());
            var parent = p.getParent();
            if (parent != null) {
                assertTrue(parent.isAbsolute(), "parent of an absolute path must remain absolute");
            }
            p = parent;
        }
        assertEquals(List.of("/aa/bb/cc/", "/aa/bb/", "/aa/", "/"), chain);
    }

    @Test
    void getParentOfSingleElementRelativePathIsNull() {
        var fs = newFileSystem("bucket");
        assertNull(fs.getPath("solo").getParent());
    }

    // ----- #4 normalize preserves leading ".." and never strips the root -----

    @Test
    void normalizePreservesLeadingDotDotInRelativePath() {
        var fs = newFileSystem("bucket");
        assertEquals("../foo", fs.getPath("../foo").normalize().toString());
        assertEquals("../../foo", fs.getPath("../../foo").normalize().toString());
    }

    @Test
    void normalizeKeepsAbsolutePathAbsolute() {
        var fs = newFileSystem("bucket");
        var normalized = fs.getPath("/../foo").normalize();
        assertTrue(normalized.isAbsolute());
        assertEquals("/foo", normalized.toString());
    }

    // ----- #5 equals/hashCode/compareTo compare the abstract path -----

    @Test
    void equalsDoesNotNormalizeOrConflateAbsoluteness() {
        var fs = newFileSystem("bucket");
        assertFalse(fs.getPath("dir/").equals(fs.getPath("/dir/")),
            "a relative path must not equal the corresponding absolute path");
        assertFalse(fs.getPath("/a/./b").equals(fs.getPath("/a/b")),
            "equals must not eliminate '.' names");
        assertEquals(fs.getPath("/a/b/"), fs.getPath("/a/b/"));
    }

    // ----- #6 iterator does not return the root component -----

    @Test
    void iteratorDoesNotEmitRootComponent() {
        var fs = newFileSystem("bucket");
        var elements = new ArrayList<String>();
        fs.getPath("/dir1/dir2/object").iterator().forEachRemaining(p -> elements.add(p.toString()));
        assertEquals(List.of("dir1/", "dir2/", "object"), elements);
    }

    // ----- #7 resolveSibling is null-safe -----

    @Test
    void resolveSiblingOnPathWithoutParentReturnsOther() {
        var fs = newFileSystem("bucket");
        var other = fs.getPath("other");
        // Empty path has no parent; per the contract resolveSibling returns other.
        assertEquals(other, fs.getPath("").resolveSibling(other));
        // Single-element relative path has no parent.
        assertEquals(other, fs.getPath("solo").resolveSibling(other));
    }

    // ----- #21 closeFileSystem is thread-safe and idempotent (#773) -----

    @Test
    void closeFileSystemIsThreadSafe() throws Exception {
        var provider = new S3FileSystemProvider();
        int threads = 16;
        var pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < 200; i++) {
                var fs = (S3FileSystem) provider.getPath(URI.create("s3://bucket-" + i)).getFileSystem();
                var start = new CountDownLatch(1);
                var error = new AtomicReference<Throwable>();
                var done = new CountDownLatch(threads);
                for (int t = 0; t < threads; t++) {
                    pool.submit(() -> {
                        try {
                            start.await();
                            provider.closeFileSystem(fs);
                        } catch (Throwable e) {
                            error.compareAndSet(null, e);
                        } finally {
                            done.countDown();
                        }
                    });
                }
                start.countDown();
                assertTrue(done.await(30, TimeUnit.SECONDS), "close tasks did not finish");
                if (error.get() != null) {
                    throw new AssertionError("concurrent closeFileSystem threw", error.get());
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // ----- #20 DirectoryStream.iterator() may only be called once -----
    // (See S3DirectoryStreamTest for iteration behavior; this pins the second-call contract via a
    //  lightweight direct construction is not possible without a client, so it is covered there.)

    // ----- #12 getFileStore returns a real, non-null store -----

    @Test
    void getFileStoreIsNonNull() {
        var provider = new S3FileSystemProvider();
        var fs = (S3FileSystem) provider.getPath(URI.create("s3://bucket")).getFileSystem();
        try {
            var store = provider.getFileStore(fs.getPath("/foo"));
            assertNotNull(store);
            assertEquals("bucket", store.name());
            assertEquals("s3", store.type());
            assertFalse(store.isReadOnly());
            assertTrue(store.supportsFileAttributeView("basic"));
        } finally {
            provider.closeFileSystem(fs);
        }
    }

    // ----- newFileSystem / getFileSystem / getPath contract (docs/newfilesystem-contract-fix.md) -----

    @Test
    void getFileSystemThrowsWhenNeverCreated() {
        var provider = new S3FileSystemProvider();
        assertThrows(java.nio.file.FileSystemNotFoundException.class,
            () -> provider.getFileSystem(URI.create("s3://uncreated-bucket")));
    }

    @Test
    void getPathMaterializesViewReturnedByGetFileSystem() {
        var provider = new S3FileSystemProvider();
        var path = provider.getPath(URI.create("s3://lazy-bucket/key"));
        assertNotNull(path);
        // getPath created a lazy view, so getFileSystem now returns it (no FileSystemNotFoundException).
        var fs = (S3FileSystem) provider.getFileSystem(URI.create("s3://lazy-bucket"));
        assertSame(path.getFileSystem(), fs);
        provider.closeFileSystem(fs);
        // After close the view is gone again.
        assertThrows(java.nio.file.FileSystemNotFoundException.class,
            () -> provider.getFileSystem(URI.create("s3://lazy-bucket")));
    }

    // Note: the FileSystemAlreadyExistsException gate on a *second* newFileSystem call for the same
    // key, and the FileSystemNotFoundException / reuse behavior, are exercised end-to-end (with the
    // createBucket side effect) in S3FileSystemProviderTest#newFileSystemUri and #getFileSystem.

    // ----- #17 truncate on a writable seekable channel is supported -----
    // (Exercised end-to-end in S3WritableByteChannelTest#shouldBeSeekable and
    //  S3SeekableByteChannelTest#truncate.)

    @Test
    void toAbsolutePathReturnsSameInstanceWhenAlreadyAbsolute() {
        var fs = newFileSystem("bucket");
        var absolute = fs.getPath("/a/b");
        assertSame(absolute, absolute.toAbsolutePath());
    }
}
