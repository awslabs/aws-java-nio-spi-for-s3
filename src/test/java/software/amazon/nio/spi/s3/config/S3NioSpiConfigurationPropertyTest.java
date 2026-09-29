/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3.config;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.assertj.core.api.BDDAssertions.then;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.*;

import java.util.HashMap;
import java.util.Map;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;

/**
 * Property-based tests for S3NioSpiConfiguration streaming multipart upload configuration.
 *
 * Feature: streaming-multipart-upload, Property 10: Configuration-driven streaming option inclusion
 * **Validates: Requirements 14.1, 14.2, 14.3, 14.4, 14.5, 14.6, 14.7, 14.8, 14.10**
 */
public class S3NioSpiConfigurationPropertyTest {

    // Feature: streaming-multipart-upload, Property 10: Configuration-driven streaming option inclusion
    // **Validates: Requirements 14.1, 14.2, 14.3, 14.4, 14.5, 14.6, 14.7, 14.8, 14.10**
    @Property(tries = 100)
    void getOpenOptionsIncludesStreamingIffResolvedValueIsTrue(
            @ForAll boolean programmaticEnabled,
            @ForAll @LongRange(min = 5 * 1024 * 1024, max = 5L * 1024 * 1024 * 1024) long programmaticPartSize) {

        // Use programmatic override (highest precedence) to test the resolved value behavior
        Map<String, String> overrides = new HashMap<>();
        overrides.put(S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD_PROPERTY, String.valueOf(programmaticEnabled));
        overrides.put(S3_SPI_WRITE_MULTIPART_PART_SIZE_PROPERTY, String.valueOf(programmaticPartSize));

        var config = new S3NioSpiConfiguration(overrides);

        var options = config.getOpenOptions();
        boolean hasStreamingOption = options.stream()
            .anyMatch(o -> o.getClass().getName().contains("S3StreamingMultipartUpload"));

        // getOpenOptions() includes S3StreamingMultipartUpload iff resolved value is true
        then(hasStreamingOption).isEqualTo(programmaticEnabled);
    }

    // Feature: streaming-multipart-upload, Property 10: Configuration-driven streaming option inclusion
    // Tests override precedence: system property > env var > default
    // **Validates: Requirements 14.3, 14.4, 14.10**
    @Property(tries = 100)
    void systemPropertyOverridesEnvVar(@ForAll boolean sysPropValue) throws Exception {
        // Env var says the opposite of system property
        String envVarValue = String.valueOf(!sysPropValue);

        withEnvironmentVariable("S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD", envVarValue)
            .execute(() -> {
                restoreSystemProperties(() -> {
                    System.setProperty(S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD_PROPERTY,
                        String.valueOf(sysPropValue));

                    var config = new S3NioSpiConfiguration();
                    // System property takes precedence over env var
                    then(config.isStreamingMultipartUploadEnabled()).isEqualTo(sysPropValue);

                    var options = config.getOpenOptions();
                    boolean hasStreamingOption = options.stream()
                        .anyMatch(o -> o.getClass().getName().contains("S3StreamingMultipartUpload"));
                    then(hasStreamingOption).isEqualTo(sysPropValue);
                });
            });
    }

    // Feature: streaming-multipart-upload, Property 10: Configuration-driven streaming option inclusion
    // Tests that programmatic override > system property > env var
    // **Validates: Requirements 14.10**
    @Property(tries = 100)
    void programmaticOverridesTakeHighestPrecedence(@ForAll boolean programmaticValue) throws Exception {
        // Set env var and system property to the opposite
        String oppositeValue = String.valueOf(!programmaticValue);

        withEnvironmentVariable("S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD", oppositeValue)
            .execute(() -> {
                restoreSystemProperties(() -> {
                    System.setProperty(S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD_PROPERTY, oppositeValue);

                    Map<String, String> overrides = new HashMap<>();
                    overrides.put(S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD_PROPERTY,
                        String.valueOf(programmaticValue));

                    var config = new S3NioSpiConfiguration(overrides);
                    // Programmatic override takes highest precedence
                    then(config.isStreamingMultipartUploadEnabled()).isEqualTo(programmaticValue);

                    var options = config.getOpenOptions();
                    boolean hasStreamingOption = options.stream()
                        .anyMatch(o -> o.getClass().getName().contains("S3StreamingMultipartUpload"));
                    then(hasStreamingOption).isEqualTo(programmaticValue);
                });
            });
    }
}
