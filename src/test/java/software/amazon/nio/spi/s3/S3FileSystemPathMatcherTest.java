/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

import java.nio.file.Path;
import java.nio.file.PathMatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3FileSystemPathMatcherTest {

    @Mock
    private S3FileSystemProvider provider;
    
    @Mock
    private S3AsyncClient s3Client;
    
    @Mock
    private S3ClientProvider clientProvider;
    
    private S3FileSystem fileSystem;
    private S3NioSpiConfiguration config;

    @BeforeEach
    void setUp() {
        config = new S3NioSpiConfiguration();
        config.setBucketName("test-bucket");
        
        fileSystem = new S3FileSystem(provider, config);
        fileSystem.clientProvider = clientProvider;
        
        when(clientProvider.generateClient("test-bucket")).thenReturn(s3Client);
    }

    @Test
    void testDefaultGlobPathMatcher() {
        PathMatcher matcher = fileSystem.getPathMatcher("glob:*.txt");
        
        Path path = S3Path.getPath(fileSystem, "/file.txt");
        assertThat(matcher.matches(path)).isTrue();
        
        Path nonMatchingPath = S3Path.getPath(fileSystem, "/file.csv");
        assertThat(matcher.matches(nonMatchingPath)).isFalse();
    }

    @Test
    void testStrictPosixGlobPathMatcher() {
        PathMatcher matcher = fileSystem.getPathMatcher("strict-posix-glob:*.txt");
        
        Path path = S3Path.getPath(fileSystem, "/file.txt");
        assertThat(matcher.matches(path)).isTrue();
        
        Path nonMatchingPath = S3Path.getPath(fileSystem, "/file.csv");
        assertThat(matcher.matches(nonMatchingPath)).isFalse();
        
        // Test directory behavior - strict POSIX glob should not match across directories
        Path nestedPath = S3Path.getPath(fileSystem, "/dir/file.txt");
        assertThat(matcher.matches(nestedPath)).isFalse();
    }

    @Test
    void testStrictPosixGlobWithDoubleAsterisk() {
        PathMatcher matcher = fileSystem.getPathMatcher("strict-posix-glob:**/*.txt");
        
        Path path = S3Path.getPath(fileSystem, "/file.txt");
        assertThat(matcher.matches(path)).isTrue();
        
        Path nestedPath = S3Path.getPath(fileSystem, "/dir/file.txt");
        assertThat(matcher.matches(nestedPath)).isTrue();
        
        Path deeplyNestedPath = S3Path.getPath(fileSystem, "/dir/subdir/file.txt");
        assertThat(matcher.matches(deeplyNestedPath)).isTrue();
    }
}
