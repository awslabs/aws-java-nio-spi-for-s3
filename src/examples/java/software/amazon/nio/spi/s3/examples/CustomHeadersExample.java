/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import software.amazon.nio.spi.s3.S3FileSystem;
import software.amazon.nio.spi.s3.S3FileSystemProvider;

/**
 * Example demonstrating how to enable custom headers for client identification.
 * 
 * When custom headers are enabled, all S3 requests will include:
 * - User-Agent: aws-java-nio-spi-for-s3/[version]
 * - X-Amz-Client-Name: aws-java-nio-spi-for-s3
 * - X-Amz-Client-Version: [version]
 * 
 * This allows you to identify requests originating from this library in S3 access logs
 * and CloudTrail logs.
 */
public class CustomHeadersExample {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: CustomHeadersExample s3://bucket/path/to/file");
            System.exit(1);
        }

        String s3Uri = args[0];
        
        // Method 1: Enable custom headers programmatically
        enableCustomHeadersProgrammatically(s3Uri);
        
        // Method 2: Enable custom headers via system property
        enableCustomHeadersViaSystemProperty(s3Uri);
    }    

    /**
     * Enable custom headers programmatically by configuring the S3FileSystemProvider
     */
    private static void enableCustomHeadersProgrammatically(String s3Uri) throws IOException {
        System.out.println("=== Enabling Custom Headers Programmatically ===");
        
        // Get the S3 file system provider
        S3FileSystemProvider provider = new S3FileSystemProvider();
        
        // Get or create the file system for the bucket
        URI uri = URI.create(s3Uri);
        S3FileSystem fileSystem = (S3FileSystem) provider.getFileSystem(uri);
        
        // Enable custom headers on the client provider
        fileSystem.clientProvider().setCustomHeadersEnabled(true);
        
        // Now perform file operations - all requests will include custom headers
        Path s3Path = Paths.get(uri);
        
        if (Files.exists(s3Path)) {
            System.out.println("File exists: " + s3Path);
            System.out.println("File size: " + Files.size(s3Path) + " bytes");
        } else {
            System.out.println("File does not exist: " + s3Path);
        }
        
        System.out.println("Custom headers have been added to all S3 requests for this file system.");
    }
    
    /**
     * Enable custom headers via system property (affects all S3FileSystemProvider instances)
     */
    private static void enableCustomHeadersViaSystemProperty(String s3Uri) throws IOException {
        System.out.println("\n=== Enabling Custom Headers Via System Property ===");
        
        // Set the system property to enable custom headers globally
        System.setProperty("s3.spi.client.custom-headers.enabled", "true");
        
        // Create a new provider instance (will pick up the system property)
        S3FileSystemProvider provider = new S3FileSystemProvider();
        
        // Perform file operations - all requests will include custom headers
        Path s3Path = Paths.get(URI.create(s3Uri));
        
        if (Files.exists(s3Path)) {
            System.out.println("File exists: " + s3Path);
            
            // Read some content to demonstrate the headers are being sent
            try {
                String content = Files.readString(s3Path);
                System.out.println("File content preview: " + 
                    content.substring(0, Math.min(100, content.length())) + 
                    (content.length() > 100 ? "..." : ""));
            } catch (Exception e) {
                System.out.println("Could not read file content: " + e.getMessage());
            }
        } else {
            System.out.println("File does not exist: " + s3Path);
        }
        
        System.out.println("Custom headers are now enabled globally via system property.");
        System.out.println("All future S3FileSystemProvider instances will include custom headers.");
    }
}