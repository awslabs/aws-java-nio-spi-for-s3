/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;


import java.net.URI;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.lenient;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@ExtendWith(MockitoExtension.class)
public class S3PathTest {
    final String uriString = "s3://mybucket";
    final S3FileSystemProvider provider = new S3FileSystemProvider();

    @Mock
    S3AsyncClient mockClient;

    S3FileSystem fileSystem;
    S3Path root;
    S3Path absoluteDirectory;
    S3Path relativeDirectory;
    S3Path absoluteObject;
    S3Path relativeObject;
    S3Path withSpecialChars;

    @BeforeEach
    public void init(){
        fileSystem = provider.newFileSystem(URI.create(uriString));
        fileSystem.clientProvider(new FixedS3ClientProvider(mockClient));
        lenient().when(mockClient.headObject(any(Consumer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
        root = S3Path.getPath(fileSystem, S3Path.PATH_SEPARATOR);
        absoluteDirectory = S3Path.getPath(fileSystem, S3Path.PATH_SEPARATOR, "dir1", "dir2/");
        relativeDirectory = S3Path.getPath(fileSystem, "..", "dir3/");
        absoluteObject = S3Path.getPath(fileSystem, S3Path.PATH_SEPARATOR, "dir1", "dir2", "object");
        relativeObject = S3Path.getPath(fileSystem, "dir1", "dir2", "object");
        withSpecialChars = S3Path.getPath(fileSystem, "dir with space/and\tspecial&chars");
    }

    @AfterEach
    public void after() throws Exception {
        fileSystem.close();
    }

    @Test
    public void getPathNullFileSystem() {
        assertThrows(IllegalArgumentException.class, () -> S3Path.getPath(null, "/", "foo"));
    }

    @Test
    public void getPathNullFirst() {
        assertThrows(IllegalArgumentException.class, () -> S3Path.getPath(fileSystem, null, "foo"));
    }

    @Test
    public void getPathEmptyFirst() {
        assertThrows(IllegalArgumentException.class, () -> S3Path.getPath(fileSystem, " ", "foo"));
    }

    @Test
    public void getPathWithScheme() {
        assertEquals(fileSystem.getPath("/", "foo"), Paths.get(URI.create(uriString+"/foo")));
    }

    @Test
    public void getFileSystem() {
       assertEquals(fileSystem, root.getFileSystem());
    }

    @Test
    public void bucketName() {
        String b = "mybucket";
        assertEquals(b, root.bucketName());
        assertEquals(b, absoluteDirectory.bucketName());
        assertEquals(b, absoluteObject.bucketName());
        assertEquals(b, relativeObject.bucketName());
        assertEquals(b, relativeDirectory.bucketName());
    }

    @Test
    public void isAbsolute() {
        assertTrue(root.isAbsolute() && absoluteObject.isAbsolute() && absoluteDirectory.isAbsolute());
        assertFalse(relativeObject.isAbsolute());
        assertFalse(relativeDirectory.isAbsolute());
    }

    @Test
    public void isDirectory() {
        assertTrue(root.isDirectory());
        assertTrue(absoluteDirectory.isDirectory());
        assertTrue(relativeDirectory.isDirectory());
        assertFalse(relativeObject.isDirectory());
        assertFalse(absoluteObject.isDirectory());
    }

    @Test
    public void getRoot() {
        assertEquals(root, root.getRoot());
        assertEquals(root, absoluteObject.getRoot());
        assertEquals(root, absoluteDirectory.getRoot());

        assertNull(relativeObject.getRoot());
        assertNull(relativeDirectory.getRoot());
    }

    @Test
    public void getFileName() {
        assertEquals("object", absoluteObject.getFileName().toString());
        assertEquals("object", relativeObject.getFileName().toString());
        assertEquals("dir2/", absoluteDirectory.getFileName().toString());
        assertEquals("dir3/", relativeDirectory.getFileName().toString());

        assertNull(root.getFileName());
    }

    @Test
    public void getParent() {
        assertNull(root.getParent());


        S3Path dir1 = S3Path.getPath(fileSystem, "dir1/");
        assertEquals(dir1, absoluteDirectory.getParent());

        S3Path top = S3Path.getPath(fileSystem, "/top");
        assertEquals(root, top.getRoot());
    }

    @Test
    public void getNameCount() {
        assertEquals(0, root.getNameCount());
        assertEquals(2, absoluteDirectory.getNameCount());
        assertEquals(2, absoluteDirectory.getNameCount());
        assertEquals(3, absoluteObject.getNameCount());
        assertEquals(3, relativeObject.getNameCount());
    }

    @Test
    public void getName() {
        S3Path dir1 = S3Path.getPath(fileSystem, "dir1/");
        S3Path dir2 = S3Path.getPath(fileSystem, "dir2/");
        S3Path object = S3Path.getPath(fileSystem, "object");

        assertEquals(dir1, absoluteObject.getName(0));
        assertEquals(dir2, absoluteObject.getName(1));
        assertEquals(object, absoluteObject.getName(2));
    }

    @Test
    public void getNameNegativeIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> absoluteObject.getName(-1));
    }

    @Test
    public void getNameOOBIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> absoluteObject.getName(3));
    }

    @Test
    public void subpath() {
        S3Path dir1 = S3Path.getPath(fileSystem, "dir1/");
        S3Path rootDir1 = S3Path.getPath(fileSystem, "/dir1/");
        S3Path dir2 = S3Path.getPath(fileSystem, "dir2/");
        S3Path up = S3Path.getPath(fileSystem, "..");
        S3Path obj = S3Path.getPath(fileSystem, "object");

        assertEquals(rootDir1, absoluteDirectory.subpath(0,1));
        assertEquals(rootDir1, absoluteObject.subpath(0,1));
        assertEquals(dir1, relativeObject.subpath(0,1));
        assertEquals(up, relativeDirectory.subpath(0,1));
        assertEquals(dir2, absoluteDirectory.subpath(1,2));

        assertEquals(obj, absoluteObject.subpath(2,3));
    }

    @Test
    public void startsWith() {
        final S3Path beginning = S3Path.getPath(fileSystem, "/dir1/dir2/");
        final S3Path relativeBeginning = S3Path.getPath(fileSystem, "dir1/dir2/");

        assertTrue(absoluteObject.startsWith(absoluteObject));
        assertTrue(relativeObject.startsWith(relativeObject));
        assertTrue(absoluteObject.startsWith(beginning));
        assertTrue(relativeObject.startsWith(relativeBeginning));

        assertFalse(relativeObject.startsWith(S3Path.getPath(fileSystem, "dir1/dir2")));
        assertFalse(absoluteObject.startsWith(relativeBeginning));
        assertFalse(absoluteObject.startsWith(S3Path.getPath(provider.newFileSystem(URI.create("s3://different-bucket")), "/dir1/")));
    }

    @Test
    public void testStartsWithString() {
        assertFalse(absoluteObject.startsWith("no"));
        assertFalse(absoluteObject.startsWith("dir1"));
        assertFalse(absoluteObject.startsWith("dir1/"));
        assertFalse(absoluteObject.startsWith("/dir1"));
        assertTrue(absoluteObject.startsWith("/dir1/"));
        assertTrue(relativeDirectory.startsWith("../"));

        assertFalse(relativeObject.startsWith("no"));
        assertFalse(relativeObject.startsWith("dir1"));
        assertTrue(relativeObject.startsWith("dir1/"));
        assertFalse(relativeObject.startsWith("/dir1"));
        assertFalse(relativeObject.startsWith("/dir1/"));
    }

    @Test
    public void endsWith() {
        final S3Path object = S3Path.getPath(fileSystem, "object");
        final S3Path ending = S3Path.getPath(fileSystem, "dir2/object");
        final S3Path dir2 = S3Path.getPath(fileSystem, "dir2/");
        final S3Path dir3 = S3Path.getPath(fileSystem, "dir3/");

        assertTrue(absoluteObject.endsWith(absoluteObject));
        assertTrue(absoluteObject.endsWith(object));
        assertTrue(absoluteObject.endsWith(ending));
        assertTrue(absoluteDirectory.endsWith(dir2));

        assertFalse(absoluteDirectory.endsWith(S3Path.getPath(fileSystem, "dir2")));
        assertFalse(absoluteDirectory.endsWith(S3Path.getPath(fileSystem, "/dir2")));
        assertFalse(absoluteDirectory.endsWith(S3Path.getPath(fileSystem, "/no/")));

        assertTrue(relativeDirectory.endsWith(dir3));
    }

    @Test
    public void testEndsWithString() {
        assertTrue(absoluteObject.endsWith("object"));
        assertTrue(absoluteObject.endsWith("dir2/object"));
        assertTrue(absoluteDirectory.endsWith("dir2/"));

        assertFalse(absoluteDirectory.endsWith("dir2"));
        assertFalse(absoluteDirectory.endsWith("/dir2"));
        assertFalse(absoluteDirectory.endsWith("/no/"));

        assertTrue(relativeDirectory.endsWith("dir3/"));
    }

    @Test
    public void normalize() {
        final S3Path path = S3Path.getPath(fileSystem, "/foo/baa/foz///object");
        final S3Path path1 = S3Path.getPath(fileSystem, "/foo/baa/./foz///object");
        final S3Path path2 = S3Path.getPath(fileSystem, "/foo/baa/./../foz///object");
        final S3Path path3 = S3Path.getPath(fileSystem, "foo/baa/./../foz///object");
        final S3Path path4 = S3Path.getPath(fileSystem, "foo/baa/./../foz/.");
        final S3Path path5 = S3Path.getPath(fileSystem, "foo/baa/./../foz/..");

        assertEquals(S3Path.getPath(fileSystem, "/foo/baa/foz/object"), path.normalize());
        assertEquals(S3Path.getPath(fileSystem, "/foo/baa/foz/object"), path1.normalize());
        assertEquals(S3Path.getPath(fileSystem, "/foo/foz/object"), path2.normalize());
        assertEquals(S3Path.getPath(fileSystem, "foo/foz/object"), path3.normalize());
        assertEquals(S3Path.getPath(fileSystem, "foo/foz/"), path4.normalize());
        assertEquals(S3Path.getPath(fileSystem, "foo/"), path5.normalize());

        assertEquals(root, root.normalize());
        assertEquals(fileSystem.getPath("../dir3/"), relativeDirectory.normalize());
        assertEquals(fileSystem.getPath("/dir3/"), relativeDirectory.toAbsolutePath().normalize());
    }

    @Test
    public void resolve() {
        assertEquals(absoluteObject, root.resolve(absoluteObject));
        assertEquals(absoluteDirectory, root.resolve(absoluteDirectory));

        final S3Path empty = fileSystem.getPath("");
        assertEquals(root, root.resolve(empty));

        assertEquals(fileSystem.getPath(absoluteDirectory.toString(), "foo") , absoluteDirectory.resolve(fileSystem.getPath("foo")));
        assertEquals(fileSystem.getPath(absoluteDirectory.toString(), "foo/") , absoluteDirectory.resolve(fileSystem.getPath("foo/")));
        assertEquals(fileSystem.getPath(absoluteObject.toString(), "foo") , absoluteObject.resolve(fileSystem.getPath("foo")));
        assertEquals(fileSystem.getPath(absoluteObject.toString(), "foo/") , absoluteObject.resolve(fileSystem.getPath("foo/")));
    }

    @Test
    public void testResolveString() {
        assertEquals(root, root.resolve(""));

        assertEquals(fileSystem.getPath(absoluteDirectory.toString(), "foo") , absoluteDirectory.resolve("foo"));
        assertEquals(fileSystem.getPath(absoluteDirectory.toString(), "foo/") , absoluteDirectory.resolve("foo/"));
        assertEquals(fileSystem.getPath(absoluteObject.toString(), "foo") , absoluteObject.resolve("foo"));
        assertEquals(fileSystem.getPath(absoluteObject.toString(), "foo/") , absoluteObject.resolve("foo/"));
    }

    @Test
    public void resolveSibling() {
        S3Path other = fileSystem.getPath("other");
        final S3Path absoluteOther = fileSystem.getPath("/dir1/dir2/other");
        final S3Path empty = fileSystem.getPath("");

        assertEquals(absoluteOther, absoluteObject.resolveSibling(other));
        assertEquals(absoluteObject, absoluteOther.resolveSibling(absoluteObject));

        assertEquals(relativeObject.getParent(), relativeObject.resolveSibling(empty));

        assertEquals(relativeObject.getParent().resolve(other), relativeObject.resolveSibling(other));
    }

    @Test
    public void testResolveSiblingString() {
        final S3Path absoluteOther = fileSystem.getPath("/dir1/dir2/other");

        assertEquals(absoluteOther, absoluteObject.resolveSibling("other"));
        assertEquals(absoluteObject, absoluteOther.resolveSibling("object"));

    }

    @Test
    public void relativize() {
        S3Path abcd = fileSystem.getPath("/a/b/c/d/");
        S3Path abcdObject = fileSystem.getPath("/a/b/c/d/object");
        S3Path ab = fileSystem.getPath("/a/b/");
        S3Path abcde = fileSystem.getPath("/a/b/c/d/e/");

        S3Path bc = fileSystem.getPath("b/c/");
        S3Path bcd = fileSystem.getPath("b/c/d/");
        S3Path bcdObject = fileSystem.getPath("b/c/d/object");

        assertEquals(fileSystem.getPath(""), absoluteObject.relativize(absoluteObject));
        assertEquals(fileSystem.getPath("../.."), abcd.relativize(ab));
        assertEquals(fileSystem.getPath("e/"), abcd.relativize(abcde));

        assertEquals(fileSystem.getPath("object"), abcd.relativize(abcdObject));

        assertEquals(fileSystem.getPath("c/d/"), ab.relativize(abcd));
        assertEquals(fileSystem.getPath("c/d/object"), ab.relativize(abcdObject));

        assertEquals(fileSystem.getPath("d/"), bc.relativize(bcd));
        assertEquals(fileSystem.getPath("d/object"), bc.relativize(bcdObject));
    }

    @Test
    public void toUri() {
        final URI uri = URI.create("s3://mybucket/dir1/dir2/");
        final URI uri1 = URI.create("s3://mybucket/dir1/dir2/object");
        final URI uri2 = URI.create("s3://mybucket/");
        final URI uri3 = URI.create("s3://mybucket/dir3/");
        final URI uri4 = URI.create("s3://mybucket/dir+with+space/and%09special%26chars");

        assertEquals(uri, absoluteDirectory.toUri());
        assertEquals(uri3, relativeDirectory.toUri());
        assertEquals(uri1, relativeObject.toUri());
        assertEquals(uri1, absoluteObject.toUri());
        assertEquals(uri2, root.toUri());
        assertEquals(uri4, withSpecialChars.toUri());
    }

    @Test
    public void toAbsolutePath() {
        assertEquals(root, root.toAbsolutePath());
        assertEquals(absoluteObject, absoluteObject.toAbsolutePath());
        assertEquals(absoluteDirectory, absoluteDirectory.toAbsolutePath());
        assertEquals(fileSystem.getPath("/../dir3/"), relativeDirectory.toAbsolutePath());
    }

    @Test
    public void toRealPath() {
        assertEquals(S3Path.getPath(fileSystem, "foo/../bar").toRealPath(LinkOption.NOFOLLOW_LINKS).toString(),
                S3Path.getPath(fileSystem, "bar").toRealPath(LinkOption.NOFOLLOW_LINKS).toString());

        assertEquals(S3Path.getPath(fileSystem, "foo/../bar/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString(),
                S3Path.getPath(fileSystem, "bar/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString());

        assertEquals(S3Path.getPath(fileSystem, "foo/.././bar/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString(),
                S3Path.getPath(fileSystem, "bar/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString());

        assertEquals(S3Path.getPath(fileSystem, "foo/../bar/.").toRealPath(LinkOption.NOFOLLOW_LINKS).toString(),
                S3Path.getPath(fileSystem, "bar/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString());

        assertEquals(S3Path.getPath(fileSystem, "foo/./bar/..").toRealPath(LinkOption.NOFOLLOW_LINKS).toString(),
                S3Path.getPath(fileSystem, "foo/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString());

        assertEquals(S3Path.getPath(fileSystem, "/foo/../bar/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString(),
                S3Path.getPath(fileSystem, "/bar/").toRealPath(LinkOption.NOFOLLOW_LINKS).toString());
    }

    @Test
    public void toFile() {
        assertThrows(UnsupportedOperationException.class, () -> absoluteObject.toFile());
    }

    @Test
    public void register() {
        assertThrows(UnsupportedOperationException.class, () -> absoluteObject.register(null));
    }

    @Test
    public void iterator() {
        assertTrue(absoluteObject.iterator().hasNext());
        assertFalse(root.iterator().hasNext());

        // /dir1/dir2/object
        List<String> absoluteObjectElements = new ArrayList<>();
        absoluteObject.iterator().forEachRemaining(p -> absoluteObjectElements.add(p.toString()));
        assertEquals(3, absoluteObjectElements.size());
        assertEquals("/dir1/", absoluteObjectElements.get(0));
        assertEquals("dir2/", absoluteObjectElements.get(1));
        assertEquals("object", absoluteObjectElements.get(2));

        // /dir1/dir2/
        List<String> absoluteDirElements = new ArrayList<>();
        absoluteDirectory.iterator().forEachRemaining(p -> absoluteDirElements.add(p.toString()));
        assertEquals(2, absoluteDirElements.size());
        assertEquals("/dir1/", absoluteDirElements.get(0));
        assertEquals("dir2/", absoluteDirElements.get(1));

        // ../dir3/
        final ArrayList<String> relativeDirectoryElements = new ArrayList<>();
        relativeDirectory.iterator().forEachRemaining(p -> relativeDirectoryElements.add(p.toString()));
        assertEquals(2, relativeDirectoryElements.size());
        assertEquals("../", relativeDirectoryElements.get(0));
        assertEquals("dir3/", relativeDirectoryElements.get(1));

        // dir1/dir2/object
        final ArrayList<String> relativeObjectElements = new ArrayList<>();
        relativeObject.iterator().forEachRemaining(p -> relativeObjectElements.add(p.toString()));
        assertEquals(3, relativeObjectElements.size());
        assertEquals("dir1/", relativeObjectElements.get(0));
        assertEquals("dir2/", relativeObjectElements.get(1));
        assertEquals("object", relativeObjectElements.get(2));
    }

    @SuppressWarnings("EqualsWithItself")
    @Test
    public void compareTo() {
        assertEquals(0, root.compareTo(root));
        assertEquals(0, relativeDirectory.compareTo(relativeDirectory));

        final S3Path rootAbc = fileSystem.getPath("/a/b/c");
        final S3Path abc = fileSystem.getPath("a/b/c");
        final S3Path bbc = fileSystem.getPath("b/b/c");
        assertEquals(0, rootAbc.compareTo(abc));
        assertEquals(0, abc.compareTo(rootAbc));
        assertTrue(abc.compareTo(bbc) < 0);
        assertTrue(bbc.compareTo(abc) > 0);
    }

    @Test
    public void testEquals() {
        // true because the equals contract requires the use of realPath which uses an absolute path which is relative to
        // the working directory, which is always "/" for a bucket.
        assertEquals(S3Path.getPath(fileSystem, "dir1/"), S3Path.getPath(fileSystem, "/dir1/"));

        assertNotEquals(S3Path.getPath(fileSystem, "dir1/"), S3Path.getPath(provider.newFileSystem(URI.create("s3://foo")), "/dir1/"));

        // not equal because in s3 dir1 cannot be implied to be a directory unless it ends with "/"
        assertNotEquals(S3Path.getPath(fileSystem, "dir1/"), S3Path.getPath(fileSystem, "dir1"));
        assertNotEquals(S3Path.getPath(fileSystem, "/dir1/"), S3Path.getPath(fileSystem, "/dir1"));
    }

    @Test
    public void testHashCode() {
        final S3Path rootAbc = fileSystem.getPath("/a/b/c");
        final S3Path abc = fileSystem.getPath("a/b/c");
        assertEquals(rootAbc.hashCode(), abc.hashCode());
    }

    @Test
    public void testToString() {
        assertEquals("/", root.toString());
        assertEquals("/dir1/dir2/", absoluteDirectory.toString());
        assertEquals("/dir1/dir2/object", absoluteObject.toString());
        assertEquals("../dir3/", relativeDirectory.toString());
        assertEquals("dir1/dir2/object", relativeObject.toString());
    }

    @Test
    public void testGetKey() {
        assertEquals("", root.getKey());
        assertEquals("dir1/dir2/", absoluteDirectory.getKey());
        assertEquals("dir1/dir2/object", absoluteObject.getKey());
        assertEquals("dir3/", relativeDirectory.getKey());
        assertEquals("dir1/dir2/object", relativeObject.getKey());
    }
}
