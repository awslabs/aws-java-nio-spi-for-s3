/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class PosixLikePathRepresentationTest {
    String root = "/";
    String absoluteFile = "/foo";
    String absoluteFolder = "/foo/";
    String absoluteFolder2 = "/foo/.";
    String absoluteFolder3 = "/foo/..";
    String absoluteFile2 = "/foo/baa";
    String relativeFolder = "foo/";
    String relativeFolder2 = "./foo/";
    String relativeFolder3 = "../foo/";
    String relativeFolder4 = "foo/.";
    String relativeFolder5 = "foo/..";
    String relativeFile = "./foo";
    String relativeFile2 = "foo";
    String relativeFile3 = "../foo";



    @Test
    public void ofNullWithMore() {
        assertThrows(IllegalArgumentException.class, () -> PosixLikePathRepresentation.of(null, "foo"));
    }

    @Test
    public void ofNull(){
        assertEquals(PosixLikePathRepresentation.EMPTY_PATH, PosixLikePathRepresentation.of(null));
    }

    @Test
    public void ofEmptyWithMore(){
        assertThrows(IllegalArgumentException.class, () -> PosixLikePathRepresentation.of("", "foo"));
    }

    @Test
    public void ofEmpty() {
        assertEquals(PosixLikePathRepresentation.EMPTY_PATH, PosixLikePathRepresentation.of(" \t"));
    }

    @Test
    public void testOf() {
        assertEquals("foo", PosixLikePathRepresentation.of("foo").toString());
        assertEquals("foo", PosixLikePathRepresentation.of("foo", "", null).toString());
        assertEquals("/foo", PosixLikePathRepresentation.of("/foo", "", null).toString());
        assertEquals("/foo/", PosixLikePathRepresentation.of("/foo", "", null, "/").toString());
        assertEquals("/foo/", PosixLikePathRepresentation.of("/foo", "", null, "//").toString());
        assertEquals("/foo/", PosixLikePathRepresentation.of("/foo//").toString());
        assertEquals("/foo/baa/", PosixLikePathRepresentation.of("/foo", "baa/", null, "//").toString());
        assertEquals("/foo/baa/", PosixLikePathRepresentation.of("/foo", "baa/", "//", null).toString());


        assertEquals("foo/baa/baz", PosixLikePathRepresentation.of("foo", "baa", "baz").toString());
        assertEquals("foo/baa/baz", PosixLikePathRepresentation.of("foo/", "baa/", "//", "/baz").toString());
        assertEquals("/foo/baa/baz", PosixLikePathRepresentation.of("/foo", "baa", "baz").toString());
        assertEquals("/foo/baa/baz", PosixLikePathRepresentation.of("/foo/", "baa/", "//", "/baz").toString());
        assertEquals("/foo/baa/baz", PosixLikePathRepresentation.of("/foo/", "baa/", "///", "/baz").toString());
        assertEquals("/foo/baa/baz", PosixLikePathRepresentation.of("/foo/", "baa/", "///", "baz").toString());
        assertEquals("/foo/baa/baz/", PosixLikePathRepresentation.of("//foo/", "baa/", "///", "baz/").toString());
        assertEquals("/foo/baa/baz", PosixLikePathRepresentation.of("//foo/baa////baz").toString());


        assertEquals("/foo/./baz", PosixLikePathRepresentation.of("/foo", ".", "baz").toString());
        assertEquals("../baz", PosixLikePathRepresentation.of("..", "baz").toString());
        assertEquals("/foo/baz/..", PosixLikePathRepresentation.of("/", "foo", "baz", "..").toString());
        assertEquals("/foo/baz/.", PosixLikePathRepresentation.of("/", "foo", "baz", ".").toString());
    }

    @Test
    public void isRoot() {
        assertTrue(PosixLikePathRepresentation.of(root).isRoot());
        assertTrue(PosixLikePathRepresentation.ROOT.isRoot());

        assertFalse(PosixLikePathRepresentation.EMPTY_PATH.isRoot());
        assertFalse(PosixLikePathRepresentation.of(absoluteFile).isRoot());
        assertFalse(PosixLikePathRepresentation.of(absoluteFile2).isRoot());
        assertFalse(PosixLikePathRepresentation.of(absoluteFolder).isRoot());
        assertFalse(PosixLikePathRepresentation.of(absoluteFolder2).isRoot());
        assertFalse(PosixLikePathRepresentation.of(absoluteFolder3).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder2).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder3).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder4).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder5).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFile).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFile2).isRoot());
        assertFalse(PosixLikePathRepresentation.of(relativeFile3).isRoot());
    }

    @Test
    public void isAbsolute() {
        assertTrue(PosixLikePathRepresentation.of(root).isAbsolute());
        assertTrue(PosixLikePathRepresentation.ROOT.isAbsolute());
        assertTrue(PosixLikePathRepresentation.of(absoluteFile).isAbsolute());
        assertTrue(PosixLikePathRepresentation.of(absoluteFile2).isAbsolute());
        assertTrue(PosixLikePathRepresentation.of(absoluteFolder).isAbsolute());
        assertTrue(PosixLikePathRepresentation.of(absoluteFolder2).isAbsolute());
        assertTrue(PosixLikePathRepresentation.of(absoluteFolder3).isAbsolute());

        assertFalse(PosixLikePathRepresentation.EMPTY_PATH.isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder).isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder2).isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder3).isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder4).isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFolder5).isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFile).isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFile2).isAbsolute());
        assertFalse(PosixLikePathRepresentation.of(relativeFile3).isAbsolute());
    }

    @Test
    public void isDirectory() {
        assertTrue(PosixLikePathRepresentation.of(root).isDirectory());
        assertTrue(PosixLikePathRepresentation.ROOT.isDirectory());
        assertTrue(PosixLikePathRepresentation.of(absoluteFolder).isDirectory());
        assertTrue(PosixLikePathRepresentation.of(absoluteFolder2).isDirectory());
        assertTrue(PosixLikePathRepresentation.of(absoluteFolder3).isDirectory());
        assertTrue(PosixLikePathRepresentation.EMPTY_PATH.isDirectory());
        assertTrue(PosixLikePathRepresentation.of(relativeFolder).isDirectory());
        assertTrue(PosixLikePathRepresentation.of(relativeFolder2).isDirectory());
        assertTrue(PosixLikePathRepresentation.of(relativeFolder3).isDirectory());
        assertTrue(PosixLikePathRepresentation.of(relativeFolder4).isDirectory());
        assertTrue(PosixLikePathRepresentation.of(relativeFolder5).isDirectory());

        assertFalse(PosixLikePathRepresentation.of(absoluteFile).isDirectory());
        assertFalse(PosixLikePathRepresentation.of(absoluteFile2).isDirectory());
        assertFalse(PosixLikePathRepresentation.of(relativeFile).isDirectory());
        assertFalse(PosixLikePathRepresentation.of(relativeFile2).isDirectory());
        assertFalse(PosixLikePathRepresentation.of(relativeFile3).isDirectory());
    }

    @Test
    public void hasTrailingSeparator() {
        assertTrue(PosixLikePathRepresentation.of(absoluteFolder).hasTrailingSeparator());
        assertFalse(PosixLikePathRepresentation.of(absoluteFolder2).hasTrailingSeparator());
        assertFalse(PosixLikePathRepresentation.of(absoluteFolder3).hasTrailingSeparator());
    }

    @Test
    public void elements() {
        assertEquals(0, PosixLikePathRepresentation.of(root).elements().size());

        assertEquals(1, PosixLikePathRepresentation.of(absoluteFile).elements().size());
        assertEquals("foo", PosixLikePathRepresentation.of(absoluteFile).elements().get(0));

        assertEquals(2, PosixLikePathRepresentation.of(absoluteFile2).elements().size());
        assertEquals("foo", PosixLikePathRepresentation.of(absoluteFile2).elements().get(0));
        assertEquals("baa", PosixLikePathRepresentation.of(absoluteFile2).elements().get(1));

        assertEquals(1, PosixLikePathRepresentation.of(absoluteFolder).elements().size());
        assertEquals("foo", PosixLikePathRepresentation.of(absoluteFolder).elements().get(0));

        assertEquals(2, PosixLikePathRepresentation.of(absoluteFolder2).elements().size());
        assertEquals("foo", PosixLikePathRepresentation.of(absoluteFolder2).elements().get(0));
        assertEquals(".", PosixLikePathRepresentation.of(absoluteFolder2).elements().get(1));
    }
}