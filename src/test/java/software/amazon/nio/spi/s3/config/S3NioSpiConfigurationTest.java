/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_ACCESS_KEY_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_REGION_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_SECRET_ACCESS_KEY_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY;
import static org.assertj.core.api.BDDAssertions.then;

import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.*;

public class S3NioSpiConfigurationTest {

    S3NioSpiConfiguration config = new S3NioSpiConfiguration();
    Properties overrides = new Properties();
    Properties badOverrides = new Properties();
    S3NioSpiConfiguration overriddenConfig;
    S3NioSpiConfiguration badOverriddenConfig;


    @BeforeEach
    public void setup(){
        overrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "1111");
        overrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, "2");
        overriddenConfig = new S3NioSpiConfiguration(overrides);

        badOverrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, "abcd");
        badOverrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "abcd");
        badOverriddenConfig = new S3NioSpiConfiguration(badOverrides);
    }

        public void constructors() {
        then(String.valueOf(String.valueOf(config.getMaxFragmentNumber()))).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT);
        then(String.valueOf(String.valueOf(config.getMaxFragmentSize()))).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT);
    }


    @Test
    public void overridesAsMap() {
        assertThrows(NullPointerException.class, () -> new S3NioSpiConfiguration((Map)null));

        Map<String, String> map = new HashMap<>();
        map.put(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "1212");
        S3NioSpiConfiguration c = new S3NioSpiConfiguration(map);

        assertEquals(1212, c.getMaxFragmentSize());
    }

    @Test
    public void getS3SpiReadMaxFragmentSize() {
        assertEquals(Integer.parseInt(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT), config.getMaxFragmentSize());

        assertEquals(1111, overriddenConfig.getMaxFragmentSize());
        assertEquals(Integer.parseInt(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT), badOverriddenConfig.getMaxFragmentSize());
    }

    @Test
    public void getS3SpiReadMaxFragmentNumber() {
        assertEquals(Integer.parseInt(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT), config.getMaxFragmentNumber());

        assertEquals(2, overriddenConfig.getMaxFragmentNumber());
        assertEquals(Integer.parseInt(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT), badOverriddenConfig.getMaxFragmentNumber());
    }

    @Test
    public void getRegion() {
        assertNull(new S3NioSpiConfiguration().getRegion());

        Properties env = new Properties();
        env.setProperty(AWS_REGION_PROPERTY, "region1");

        assertEquals("region1", new S3NioSpiConfiguration(env).getRegion());
    }

    @Test
    public void getEndpointProtocol() {
        assertEquals(S3NioSpiConfiguration.S3_SPI_ENDPOINT_PROTOCOL_DEFAULT, new S3NioSpiConfiguration().getEndpointProtocol());
        assertEquals("https", overriddenConfig.getEndpointProtocol());
        assertEquals(S3NioSpiConfiguration.S3_SPI_ENDPOINT_PROTOCOL_DEFAULT, badOverriddenConfig.getEndpointProtocol());
    }

    @Test
    public void getCredentials() {
        assertNull(new S3NioSpiConfiguration().getCredentials());

        Properties env = new Properties();
        env.setProperty(AWS_ACCESS_KEY_PROPERTY, "envkey");
        env.put(AWS_SECRET_ACCESS_KEY_PROPERTY, "envsecret");

        AwsCredentials credentials = new S3NioSpiConfiguration(env).getCredentials();
        assertEquals("envkey", credentials.accessKeyId());
        assertEquals("envsecret", credentials.secretAccessKey());
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
