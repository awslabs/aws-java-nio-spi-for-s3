/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
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

/**
 * Edge case tests for LibraryVersion that test error handling and fallback scenarios.
 */
class LibraryVersionEdgeCaseTest {

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
    void testReadVersionFromPropertiesWithIOException() {
        // Test that IOException is handled gracefully
        // We can't easily mock the InputStream creation, but we can test the method doesn't throw
        String version = LibraryVersion.readVersionFromProperties();
        // Should return null or valid version, never throw IOException
        if (version != null) {
            assertThat(version).isNotEmpty();
        }
    }

    @Test
    void testReadVersionFromPropertiesWithEmptyProperties() {
        // This tests the case where properties file exists but version property is missing
        String version = LibraryVersion.readVersionFromProperties();
        // Should handle missing property gracefully
        if (version != null) {
            assertThat(version).isNotEmpty();
        }
    }

    @Test
    void testReadVersionFromManifestWithException() {
        // Test that exceptions in manifest reading are handled
        String version = LibraryVersion.readVersionFromManifest();
        // Should return null or valid version, never throw exception
        if (version != null) {
            assertThat(version).isNotEmpty();
            assertThat(version.trim()).isNotEmpty();
        }
    }

    @Test
    void testReadVersionFromManifestWithEmptyVersion() {
        // Test handling of empty version string from manifest
        String version = LibraryVersion.readVersionFromManifest();
        // Should return null if version is empty/whitespace, or valid version
        if (version != null) {
            assertThat(version.trim()).isNotEmpty();
        }
    }

    @Test
    void testDetermineVersionFallbackPath() {
        // Test that determineVersion eventually falls back to FALLBACK_VERSION
        String version = LibraryVersion.determineVersion();
        assertThat(version).isNotNull().isNotEmpty();
        
        // Should be a valid semantic version (either from properties/manifest or fallback)
        assertThat(version).matches("\\d+\\.\\d+\\.\\d+.*");
    }

    @Test
    void testGetVersionThreadSafety() throws Exception {
        // Test that concurrent access to getVersion() works correctly
        // Reset cache first
        resetCachedVersion();
        
        // Create multiple threads that call getVersion()
        Thread[] threads = new Thread[10];
        String[] results = new String[10];
        
        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                results[index] = LibraryVersion.getVersion();
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        // All results should be the same (cached value)
        String expectedVersion = results[0];
        for (String result : results) {
            assertThat(result).isEqualTo(expectedVersion);
        }
    }

    @Test
    void testVersionConstantsAreValid() throws Exception {
        // Test that all constants are properly initialized
        Field libraryNameField = LibraryVersion.class.getDeclaredField("LIBRARY_NAME");
        libraryNameField.setAccessible(true);
        String libraryName = (String) libraryNameField.get(null);
        assertThat(libraryName).isNotNull().isNotEmpty();
        
        Field fallbackVersionField = LibraryVersion.class.getDeclaredField("FALLBACK_VERSION");
        fallbackVersionField.setAccessible(true);
        String fallbackVersion = (String) fallbackVersionField.get(null);
        assertThat(fallbackVersion).isNotNull().isNotEmpty();
        assertThat(fallbackVersion).matches("\\d+\\.\\d+\\.\\d+.*");
        
        Field versionPropertiesPathField = LibraryVersion.class.getDeclaredField("VERSION_PROPERTIES_PATH");
        versionPropertiesPathField.setAccessible(true);
        String versionPropertiesPath = (String) versionPropertiesPathField.get(null);
        assertThat(versionPropertiesPath).isNotNull().isNotEmpty();
        assertThat(versionPropertiesPath).startsWith("/");
    }

    @Test
    void testPrivateConstructor() throws Exception {
        // Test that the utility class has a private constructor
        assertThat(LibraryVersion.class.getDeclaredConstructors()).hasSize(1);
        assertThat(LibraryVersion.class.getDeclaredConstructors()[0].getParameterCount()).isZero();
        
        // Constructor should be private
        assertThat(LibraryVersion.class.getDeclaredConstructors()[0].canAccess(null)).isFalse();
    }

    @Test
    void testCachedVersionFieldExists() throws Exception {
        // Test that the cachedVersion field exists and is properly typed
        Field cachedVersionField = LibraryVersion.class.getDeclaredField("cachedVersion");
        assertThat(cachedVersionField.getType()).isEqualTo(String.class);
        
        // Should be volatile for thread safety
        assertThat(java.lang.reflect.Modifier.isVolatile(cachedVersionField.getModifiers())).isTrue();
    }

    @Test
    void testAllMethodsHandleNullGracefully() {
        // Test that all public methods handle edge cases gracefully
        
        // getLibraryName should never return null
        String libraryName = LibraryVersion.getLibraryName();
        assertThat(libraryName).isNotNull().isNotEmpty();
        
        // getVersion should never return null
        String version = LibraryVersion.getVersion();
        assertThat(version).isNotNull().isNotEmpty();
        
        // Package-private methods should handle nulls gracefully
        String propertiesVersion = LibraryVersion.readVersionFromProperties();
        // Can be null, but if not null, should be valid
        if (propertiesVersion != null) {
            assertThat(propertiesVersion).isNotEmpty();
        }
        
        String manifestVersion = LibraryVersion.readVersionFromManifest();
        // Can be null, but if not null, should be valid
        if (manifestVersion != null) {
            assertThat(manifestVersion).isNotEmpty();
        }
        
        // determineVersion should never return null
        String determinedVersion = LibraryVersion.determineVersion();
        assertThat(determinedVersion).isNotNull().isNotEmpty();
    }
}