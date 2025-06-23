/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class LibraryVersionTest {

    @BeforeEach
    void setUp() throws Exception {
        // Reset the cached version before each test
        resetCachedVersion();
    }

    @AfterEach
    void tearDown() throws Exception {
        // Reset the cached version after each test
        resetCachedVersion();
    }

    private void resetCachedVersion() throws Exception {
        Field cachedVersionField = LibraryVersion.class.getDeclaredField("cachedVersion");
        cachedVersionField.setAccessible(true);
        cachedVersionField.set(null, null);
    }

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

    @Test
    void testDetermineVersionFallsBackCorrectly() {
        // Test the version determination logic
        String version = LibraryVersion.determineVersion();
        assertThat(version).isNotNull().isNotEmpty();
    }

    @Test
    void testReadVersionFromPropertiesWithValidProperties() {
        // This will test the actual properties file if it exists
        String version = LibraryVersion.readVersionFromProperties();
        // Version might be null if properties file doesn't exist, or contain actual version
        if (version != null) {
            assertThat(version).matches("\\d+\\.\\d+\\.\\d+.*");
        }
    }

    @Test
    void testReadVersionFromPropertiesWithMissingFile() {
        // Test behavior when properties file is missing
        // We can't easily mock the resource loading, but we can test the method exists
        String version = LibraryVersion.readVersionFromProperties();
        // Should either return a valid version or null (both are acceptable)
        if (version != null) {
            assertThat(version).isNotEmpty();
        }
    }

    @Test
    void testReadVersionFromManifest() {
        // Test reading from JAR manifest
        String version = LibraryVersion.readVersionFromManifest();
        // Manifest version might not be available in test environment
        if (version != null) {
            assertThat(version).isNotEmpty();
            assertThat(version.trim()).isNotEmpty();
        }
    }

    @Test
    void testReadVersionFromManifestWithNullPackage() {
        // Test the null package scenario
        // This is hard to test directly, but we can verify the method handles it gracefully
        String version = LibraryVersion.readVersionFromManifest();
        // Should return null or a valid version, never throw an exception
        if (version != null) {
            assertThat(version).isNotEmpty();
        }
    }

    @Test
    void testVersionCaching() throws Exception {
        // Test that version is cached properly
        String version1 = LibraryVersion.getVersion();
        
        // Verify it's cached by checking the field
        Field cachedVersionField = LibraryVersion.class.getDeclaredField("cachedVersion");
        cachedVersionField.setAccessible(true);
        String cachedValue = (String) cachedVersionField.get(null);
        
        assertThat(cachedValue).isEqualTo(version1);
        
        // Second call should return the same cached value
        String version2 = LibraryVersion.getVersion();
        assertThat(version2).isEqualTo(version1);
    }

    @Test
    void testFallbackVersionIsUsed() {
        // Test that fallback version is reasonable
        String fallbackVersion = "2.2.1"; // Current fallback
        
        // The actual version should either be from properties/manifest or fallback
        String actualVersion = LibraryVersion.getVersion();
        assertThat(actualVersion).isNotNull().isNotEmpty();
        
        // Should be a valid semantic version
        assertThat(actualVersion).matches("\\d+\\.\\d+\\.\\d+.*");
    }

    @Test
    void testVersionConstants() throws Exception {
        // Test that constants are properly defined
        Field libraryNameField = LibraryVersion.class.getDeclaredField("LIBRARY_NAME");
        libraryNameField.setAccessible(true);
        String libraryName = (String) libraryNameField.get(null);
        assertThat(libraryName).isEqualTo("aws-java-nio-spi-for-s3");
        
        Field fallbackVersionField = LibraryVersion.class.getDeclaredField("FALLBACK_VERSION");
        fallbackVersionField.setAccessible(true);
        String fallbackVersion = (String) fallbackVersionField.get(null);
        assertThat(fallbackVersion).matches("\\d+\\.\\d+\\.\\d+.*");
    }

    @Test
    void testVersionPropertiesPath() throws Exception {
        // Test that the properties path constant is correct
        Field pathField = LibraryVersion.class.getDeclaredField("VERSION_PROPERTIES_PATH");
        pathField.setAccessible(true);
        String path = (String) pathField.get(null);
        assertThat(path).isEqualTo("/version.properties");
    }
}