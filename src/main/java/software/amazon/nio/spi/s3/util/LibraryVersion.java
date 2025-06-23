/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to retrieve the library version information.
 */
public final class LibraryVersion {
    
    private static final String LIBRARY_NAME = "aws-java-nio-spi-for-s3";
    private static final String FALLBACK_VERSION = "2.2.1";
    private static final String VERSION_PROPERTIES_PATH = "/version.properties";
    
    private static volatile String cachedVersion;
    
    private LibraryVersion() {
        // Utility class
    }
    
    /**
     * Gets the library name.
     * 
     * @return the library name
     */
    public static String getLibraryName() {
        return LIBRARY_NAME;
    }
    
    /**
     * Gets the library version, attempting to read from version.properties first,
     * then falling back to the JAR manifest, and finally to a hardcoded fallback.
     * 
     * @return the library version
     */
    public static String getVersion() {
        if (cachedVersion == null) {
            synchronized (LibraryVersion.class) {
                if (cachedVersion == null) {
                    cachedVersion = determineVersion();
                }
            }
        }
        return cachedVersion;
    }
    
    static String determineVersion() {
        // Try to read from version.properties first
        String version = readVersionFromProperties();
        if (version != null) {
            return version;
        }
        
        // Try to read from JAR manifest
        version = readVersionFromManifest();
        if (version != null) {
            return version;
        }
        
        // Fallback to hardcoded version
        return FALLBACK_VERSION;
    }
    
    static String readVersionFromProperties() {
        try (InputStream is = LibraryVersion.class.getResourceAsStream(VERSION_PROPERTIES_PATH)) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                return props.getProperty("version");
            }
        } catch (IOException e) {
            // Ignore and try next method
        }
        return null;
    }
    
    static String readVersionFromManifest() {
        try {
            Package pkg = LibraryVersion.class.getPackage();
            if (pkg != null) {
                String version = pkg.getImplementationVersion();
                if (version != null && !version.trim().isEmpty()) {
                    return version;
                }
            }
        } catch (Exception e) {
            // Ignore and fall back
        }
        return null;
    }
}