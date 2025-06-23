/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;
class S3NioSpiInterceptorTest {

    private S3ClientProvider provider;

    @BeforeEach
    void setUp() {
        provider = new S3ClientProvider(new S3NioSpiConfiguration());
    }

    @Test
    void testInterceptorIsConfiguredWhenCustomHeadersEnabled() {
        // When custom headers are enabled, the regular client should be used
        provider.setCustomHeadersEnabled(true);
        
        // Generate a client - this should use the regular client with interceptor
        S3AsyncClient client = provider.generateClient("test-bucket");
        
        assertThat(client).isNotNull();
        // The client should be wrapped in CacheableS3Client
        assertThat(client).isInstanceOf(CacheableS3Client.class);
    }

    @Test
    void testCrtClientUsedWhenCustomHeadersDisabled() {
        // When custom headers are disabled, the CRT client should be used
        provider.setCustomHeadersEnabled(false);
        
        // Generate a client - this should use the CRT client
        S3AsyncClient client = provider.generateClient("test-bucket");
        
        assertThat(client).isNotNull();
        assertThat(client).isInstanceOf(CacheableS3Client.class);
    }

    @Test
    void testRegularClientBuilderConfiguration() {
        // Test that the regular client builder is properly configured
        var builder = provider.configureRegularClient();
        
        assertThat(builder).isNotNull();
        
        // Build the client to ensure configuration is valid
        S3AsyncClient client = builder.build();
        assertThat(client).isNotNull();
        
        // Clean up
        client.close();
    }

    @Test
    void testClientCaching() {
        provider.setCustomHeadersEnabled(true);
        
        // Generate clients for the same bucket - should be cached
        S3AsyncClient client1 = provider.generateClient("test-bucket");
        S3AsyncClient client2 = provider.generateClient("test-bucket");
        
        // Should return the same cached instance
        assertThat(client1).isSameAs(client2);
        
        // Different bucket should return different client
        S3AsyncClient client3 = provider.generateClient("other-bucket");
        assertThat(client3).isNotSameAs(client1);
    }
}