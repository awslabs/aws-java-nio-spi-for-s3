/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LibraryVersionTest {

    @Test
    void testGetLibraryName() {
        String libraryName = LibraryVersion.getLibraryName();
        assertThat(libraryName).isEqualTo("aws-java-nio-spi-for-s3");
    }

    @Test
    void testGetVersionReturnsNonNull() {
        String version = LibraryVersion.getVersion();
        assertThat(version).isNotNull().isNotEmpty();
    }

    @Test
    void testGetVersionIsConsistent() {
        // Multiple calls should return the same cached version
        String version1 = LibraryVersion.getVersion();
        String version2 = LibraryVersion.getVersion();
        assertThat(version1).isEqualTo(version2);
    }

    @Test
    void testVersionFormat() {
        String version = LibraryVersion.getVersion();
        // Version should follow semantic versioning pattern (at least X.Y.Z)
        assertThat(version).matches("\\d+\\.\\d+\\.\\d+.*");
    }

    @Test
    void testVersionNotEmpty() {
        String version = LibraryVersion.getVersion();
        assertThat(version.trim()).isNotEmpty();
    }
}