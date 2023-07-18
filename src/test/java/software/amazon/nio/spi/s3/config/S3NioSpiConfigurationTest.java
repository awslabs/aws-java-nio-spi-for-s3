/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3.config;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import static org.assertj.core.api.BDDAssertions.entry;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.*;

public class S3NioSpiConfigurationTest {

    S3NioSpiConfiguration config = new S3NioSpiConfiguration();
    Properties overrides = new Properties();
    Properties badOverrides = new Properties();
    S3NioSpiConfiguration overriddenConfig;
    S3NioSpiConfiguration badOverriddenConfig;

    @BeforeEach
    public void setup() {
        overrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "1111");
        overrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, "2");
        overriddenConfig = new S3NioSpiConfiguration(overrides);

        badOverrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, "abcd");
        badOverrides.setProperty(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "abcd");
        badOverriddenConfig = new S3NioSpiConfiguration(badOverrides);
    }

    @Test
    public void constructors() {
        then(config).isInstanceOf(Map.class);
        then(config.getMaxFragmentNumber()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT);
        then(config.getMaxFragmentSize()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT);
        then(config.getEndpointProtocol()).isEqualTo("https");
        then(config.getEndpoint()).isEmpty();
        then(config.getRegion()).isNull();
        then(config.getCredentials()).isNull();
    }

    @Test
    public void overridesAsMap() {
        assertThrows(NullPointerException.class, () -> new S3NioSpiConfiguration((Map) null));

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
        then(C.withRegion("\tregion2 ")).isSameAs(C);
        then(C.getRegion()).isEqualTo("region2");
        then(C.withRegion(" \t ").getRegion()).isNull();
        then(C.withRegion("").getRegion()).isNull();
        then(C.withRegion(null).getRegion()).isNull();
    }

    @Test
    public void withAndGetEndpoint() {
        then(config.withEndpoint("somewhere.com:8000")).isSameAs(config);
        then(config.getEndpoint()).isEqualTo("somewhere.com:8000");
        then(config.withEndpoint(" somewhere.com:8080\t").getEndpoint()).isEqualTo("somewhere.com:8080");
        then(config.withEndpoint("   ").getEndpoint()).isEqualTo("");
        then(config.withEndpoint(null).getEndpoint()).isEqualTo("");

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
    }

    @Test
    public void withAndGetEndpointProtocol() {
        then(config.withEndpointProtocol("http")).isSameAs(config);
        then(config.getEndpointProtocol()).isEqualTo("http");
        then(config.withEndpointProtocol("  http\n").getEndpointProtocol()).isEqualTo("http");
        then(overriddenConfig.getEndpointProtocol()).isEqualTo("https");
        then(badOverriddenConfig.getEndpointProtocol()).isEqualTo(S3_SPI_ENDPOINT_PROTOCOL_DEFAULT);

        try {
            config.withEndpointProtocol("ftp");
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("endpoint prococol must be one of ('http', 'https')");
        }
    }

    @Test
    public void withAndGetMaxFragmentNumber() {
        then(config.withMaxFragmentNumber(1000)).isSameAs(config);
        then(config.getMaxFragmentNumber()).isEqualTo(1000);

        try {
            config.withMaxFragmentNumber(-1);
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("maxFragmentNumber must be positive");
        }
    }

    @Test
    public void withAndGetMaxFragmentSize() {
        then(config.withMaxFragmentSize(4000)).isSameAs(config);
        then(config.getMaxFragmentSize()).isEqualTo(4000);

        try {
            config.withMaxFragmentSize(-1);
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("maxFragmentSize must be positive");
        }
    }

    @Test
    public void withAndGetPlainCredentials() {
        then(config.withCredentials("akey", "asecret")).isSameAs(config);

        AwsCredentials credentials = config.getCredentials();
        then(credentials.accessKeyId()).isEqualTo("akey");
        then(credentials.secretAccessKey()).isEqualTo("asecret");

        credentials = config.withCredentials("anotherkey", "anothersecret").getCredentials();
        then(credentials.accessKeyId()).isEqualTo("anotherkey");
        then(credentials.secretAccessKey()).isEqualTo("anothersecret");

        then(config.withCredentials(null, "something").getCredentials()).isNull();

        try {
            config.withCredentials("akey", null);
            fail("missing sanity check");
        } catch (IllegalArgumentException x) {
            then(x).hasMessage("secretAccessKey can not be null");
        }
    }

    @Test
    public void getHttpProtocolFromEnvironment() throws Exception {
        withEnvironmentVariable("S3_SPI_ENDPOINT_PROTOCOL", "http")
        .execute(() -> {
            then(new S3NioSpiConfiguration().getEndpointProtocol()).isEqualTo("http");

            restoreSystemProperties(() -> {
                System.setProperty(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, "https");
                then(new S3NioSpiConfiguration().getEndpointProtocol()).isEqualTo("https");
            });
        });
    }

    @Test
    public void withAndGetCredentials() {
        final AwsCredentials C1 = AwsBasicCredentials.create("key", "secret");
        final AwsCredentials C2 = AwsBasicCredentials.create("key", "secret");

        then(config.withCredentials(C1)).isSameAs(config);
        AwsCredentials c = config.getCredentials();
        then(c.accessKeyId()).isEqualTo(C1.accessKeyId());
        then(c.secretAccessKey()).isEqualTo(C1.secretAccessKey());

        then(config.withCredentials(C2)).isSameAs(config);
        c = config.getCredentials();
        then(c.accessKeyId()).isEqualTo(C2.accessKeyId());
        then(c.secretAccessKey()).isEqualTo(C2.secretAccessKey());

        then(config.withCredentials(null)).doesNotContainKeys(
            AWS_ACCESS_KEY_PROPERTY, AWS_SECRET_ACCESS_KEY_PROPERTY
        );
        then(config.getCredentials()).isNull();
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
