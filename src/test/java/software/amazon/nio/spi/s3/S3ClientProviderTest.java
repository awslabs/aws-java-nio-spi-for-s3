/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

@ExtendWith(MockitoExtension.class)
public class S3ClientProviderTest {

    S3ClientProvider provider;

    @BeforeEach
    public void before() {
        provider = new S3ClientProvider(null);
    }

    @Test
    public void initialization() {
        final var s3ClientProvider = new S3ClientProvider(null);

        assertNotNull(s3ClientProvider.configuration);

        var config = new S3NioSpiConfiguration();
        assertSame(config, new S3ClientProvider(config).configuration);
    }

    @Test
    public void testGenerateAsyncClientWithNoErrors() {
        final var s3Client = provider.generateClient("test-bucket");
        assertNotNull(s3Client);
    }

    @Test
    public void testGenerateClientIsCacheableClass() {
        final var s3Client = provider.generateClient("test-bucket");
        assertInstanceOf(CacheableS3Client.class, s3Client);
    }

    @Test
    public void testGenerateClientCachesClients() {
        final var s3Client = provider.generateClient("test-bucket");
        final var s3Client2 = provider.generateClient("test-bucket");
        assertSame(s3Client, s3Client2);
    }

    @Test
    public void testClosedClientIsNotReused() {

        final var s3Client = provider.generateClient("test-bucket");
        assertNotNull(s3Client);

        // now close the client
        s3Client.close();

        // now generate a new client with the same bucket name
        final var s3Client2 = provider.generateClient("test-bucket");
        assertNotNull(s3Client2);

        // assert it is not the closed client
        assertNotSame(s3Client, s3Client2);
    }

    @Test
    public void testRegionIsPropagated() {
        // expect a non-standard region
        final var expectedRegion = Region.AP_SOUTHEAST_2;

        // set it in the provider
        provider.configuration.withRegion(expectedRegion.id());

        try (final var s3Client = provider.generateClient("test-bucket")) {
            // get the actual one from the created client
            assertNotNull(s3Client);
            final var configuration = s3Client.serviceClientConfiguration();
            assertNotNull(configuration);
            final var actualRegion = configuration.region();

            // assert it is as expected
            assertSame(expectedRegion, actualRegion);
        }
    }

    @Test
    public void generateAsyncClientByEndpointBucketCredentials() {
        // GIVEN
        // use a spy to record method calls on the builder which should be invoked by the provider
        var BUILDER = spy(S3AsyncClient.crtBuilder());
        provider.asyncClientBuilder = BUILDER;
        provider.configuration.withEndpoint("endpoint1:1010");

        // WHEN
        provider.generateClient("bucket1");

        // THEN
        verify(BUILDER, times(1)).endpointOverride(URI.create("https://endpoint1:1010"));

        // GIVEN
        BUILDER = spy(S3AsyncClient.crtBuilder());
        provider.asyncClientBuilder = BUILDER;
        provider.configuration.withEndpoint("endpoint2:2020");

        // WHEN
        provider.generateClient("bucket2");

        // THEN
        verify(BUILDER, times(1)).endpointOverride(URI.create("https://endpoint2:2020"));
    }
}
