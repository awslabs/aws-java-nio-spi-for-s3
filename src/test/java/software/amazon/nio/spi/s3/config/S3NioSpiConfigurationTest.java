/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;


import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class S3NioSpiConfigurationTest {

    S3NioSpiConfiguration config = new S3NioSpiConfiguration();
    Properties overrides = new Properties();
    Properties badOverrides = new Properties();
    S3NioSpiConfiguration overriddenConfig;
    S3NioSpiConfiguration badOverriddenConfig;

    @BeforeEach
    public void setup(){
        overrides.setProperty(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "1111");
        overrides.setProperty(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, "2");
        overriddenConfig = new S3NioSpiConfiguration(overrides);

        badOverrides.setProperty(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, "abcd");
        badOverrides.setProperty(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "abcd");
        badOverriddenConfig = new S3NioSpiConfiguration(badOverrides);
    }

    @Test
    public void getS3SpiReadMaxFragmentSize() {
        assertEquals(Integer.parseInt(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT), config.getMaxFragmentSize());

        assertEquals(1111, overriddenConfig.getMaxFragmentSize());
        assertEquals(Integer.parseInt(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT), badOverriddenConfig.getMaxFragmentSize());
    }

    @Test
    public void getS3SpiReadMaxFragmentNumber() {
        assertEquals(Integer.parseInt(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT), config.getMaxFragmentNumber());

        assertEquals(2, overriddenConfig.getMaxFragmentNumber());
        assertEquals(Integer.parseInt(S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT), badOverriddenConfig.getMaxFragmentNumber());
    }

    @Test
    public void convertPropertyNameToEnvVar() {
        String expected = "FOO_BAA_FIZZ_BUZZ";
        assertEquals(expected, config.convertPropertyNameToEnvVar("foo.baa.fizz-buzz"));

        expected = "";
        assertEquals(expected, config.convertPropertyNameToEnvVar(null));
        assertEquals(expected, config.convertPropertyNameToEnvVar("  "));
    }
}