/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.*;


//
// TODO: add withRegion(), withAccessKey(), withSecretAccessKey()
//
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

    @Test
    public void constructors() {
        then(config instanceof Map<String, String>).isTrue();
        then(config.getMaxFragmentNumber()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT);
        then(config.getMaxFragmentSize()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT);
        then(config.getEndpointProtocol()).isEqualTo("https");
        then(config.getEndpoint()).isEmpty();
        then(config.getRegion()).isNull();
        then(config.getCredentials()).isNull();
    }

    @Test
    public void fluentSetupOK() {
        //
        // TODO: break this in the dedicated withAndGetXXX methods
        //
        then(config.withMaxFragmentNumber(1000).getMaxFragmentNumber()).isEqualTo(1000);
        then(config.withMaxFragmentSize(4000).getMaxFragmentSize()).isEqualTo(4000);
        then(config.withEndpoint("somewhere.com:8000").getEndpoint()).isEqualTo("somewhere.com:8000");
        then(config.withEndpoint(" somewhere.com:8080\t").getEndpoint()).isEqualTo("somewhere.com:8080");
        then(config.withEndpoint("   ").getEndpoint()).isEqualTo("");
        then(config.withEndpoint(null).getEndpoint()).isEqualTo("");
        then(config.withEndpointProtocol("http").getEndpointProtocol()).isEqualTo("http");
        then(config.withEndpointProtocol("  http\n").getEndpointProtocol()).isEqualTo("http");
    }

    @Test
    public void fluentSetupKO() {
        //
        // TODO: break this in the dedicated withAndGetXXX methods
        //
        try {
            config.withMaxFragmentNumber(-1);
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("maxFragmentNumber must be positive");
        }
        try {
            config.withMaxFragmentSize(-1);
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("maxFragmentSize must be positive");
        }

        try {
           config.withEndpoint("noport.somewhere.com");
           fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("endpoint 'noport.somewhere.com' does not match format host:port");
        }

        try {
            config.withEndpoint("wrongport.somewhere.com:aabbcc");
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("endpoint 'wrongport.somewhere.com:aabbcc' does not match format host:port");
        }

        try {
            config.withEndpointProtocol("ftp");
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("endpoint prococol must be one of ('http', 'https')");
        }
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
        assertEquals(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT, config.getMaxFragmentSize());

        assertEquals(1111, overriddenConfig.getMaxFragmentSize());
        assertEquals(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT, badOverriddenConfig.getMaxFragmentSize());
    }

    @Test
    public void getS3SpiReadMaxFragmentNumber() {
        assertEquals(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT, config.getMaxFragmentNumber());

        assertEquals(2, overriddenConfig.getMaxFragmentNumber());
        assertEquals(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT, badOverriddenConfig.getMaxFragmentNumber());
    }

    @Test
    public void withAndGetRegion() {
        then(new S3NioSpiConfiguration().getRegion()).isNull();

        Properties env = new Properties();
        env.setProperty(AWS_REGION_PROPERTY, "region1");

        final S3NioSpiConfiguration C = new S3NioSpiConfiguration(env);
        then(C.getRegion()).isEqualTo("region1");
        then(C.withRegion("\tregion2 ")).isSameAs(C); then(C.getRegion()).isEqualTo("region2");
        then(C.withRegion(" \t ").getRegion()).isNull();
        then(C.withRegion("").getRegion()).isNull();
        then(C.withRegion(null).getRegion()).isNull();
    }

    @Test
    public void getEndpointProtocol() {
        assertEquals(S3NioSpiConfiguration.S3_SPI_ENDPOINT_PROTOCOL_DEFAULT, new S3NioSpiConfiguration().getEndpointProtocol());
        assertEquals("https", overriddenConfig.getEndpointProtocol());
        assertEquals(S3NioSpiConfiguration.S3_SPI_ENDPOINT_PROTOCOL_DEFAULT, badOverriddenConfig.getEndpointProtocol());
    }

    @Test
    public void withAndGetCredentials() {
        final S3NioSpiConfiguration C = new S3NioSpiConfiguration();

        then(C.withCredentials("akey", "asecret")).isSameAs(C);

        AwsCredentials credentials = C.getCredentials();
        then(credentials.accessKeyId()).isEqualTo("akey");
        then(credentials.secretAccessKey()).isEqualTo("asecret");

        credentials = C.withCredentials("anotherkey", "anothersecret").getCredentials();
        then(credentials.accessKeyId()).isEqualTo("anotherkey");
        then(credentials.secretAccessKey()).isEqualTo("anothersecret");

        then(C.withCredentials(null, "something").getCredentials()).isNull();

        //
        // TODO: ko path
        //
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
