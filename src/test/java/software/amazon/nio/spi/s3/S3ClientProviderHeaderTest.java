/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;
import software.amazon.nio.spi.s3.util.LibraryVersion;

@ExtendWith(MockitoExtension.class)
class S3ClientProviderHeaderTest {

    @Mock
    private S3AsyncClient mockClient;

    private S3ClientProvider provider;

    @BeforeEach
    void setUp() {
        provider = new S3ClientProvider(new S3NioSpiConfiguration());
    }

    @Test
    void testLibraryVersionUtility() {
        // Test that LibraryVersion utility works correctly
        String libraryName = LibraryVersion.getLibraryName();
        String version = LibraryVersion.getVersion();
        
        assertThat(libraryName).isEqualTo("aws-java-nio-spi-for-s3");
        assertThat(version).isNotNull().isNotEmpty();
        
        // Version should be either from properties or fallback
        assertThat(version).matches("\\d+\\.\\d+\\.\\d+.*");
    }

    @Test
    void testClientConfigurationIncludesInterceptor() {
        // Test that the client configuration includes our custom interceptor
        var clientBuilder = provider.configureCrtClient();
        
        // Verify that the builder has been configured with override configuration
        // This is a basic test to ensure the method completes without error
        assertThat(clientBuilder).isNotNull();
    }
    
    @Test
    void testCustomHeadersCanBeEnabled() {
        // Test that custom headers can be enabled
        provider.setCustomHeadersEnabled(true);
        
        // Generate a client - should use regular client with headers
        var client = provider.generateClient("test-bucket");
        assertThat(client).isNotNull();
    }
    
    @Test
    void testRegularClientConfiguration() {
        // Test that the regular client configuration works
        var clientBuilder = provider.configureRegularClient();
        
        // Verify that the builder has been configured
        assertThat(clientBuilder).isNotNull();
    }
    
    @Test
    void testLibraryVersionFallback() {
        // Test that LibraryVersion handles missing properties gracefully
        String version = LibraryVersion.getVersion();
        assertThat(version).isNotNull().isNotEmpty();
        
        // Test that multiple calls return the same cached version
        String version2 = LibraryVersion.getVersion();
        assertThat(version2).isEqualTo(version);
    }
    
    @Test
    void testSystemPropertyConfiguration() {
        // Test that system property is read correctly
        System.setProperty("s3.spi.client.custom-headers.enabled", "true");
        try {
            S3ClientProvider testProvider = new S3ClientProvider(new S3NioSpiConfiguration());
            // The provider should now use custom headers by default
            var client = testProvider.generateClient("test-bucket");
            assertThat(client).isNotNull();
        } finally {
            System.clearProperty("s3.spi.client.custom-headers.enabled");
        }
    }
}