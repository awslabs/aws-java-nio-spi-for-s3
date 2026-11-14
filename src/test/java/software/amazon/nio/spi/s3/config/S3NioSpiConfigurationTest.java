/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3.config;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.BDDAssertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.nio.spi.s3.S3OpenOption;

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
    public void constructors() throws Exception {
        then(config.asMap()).isNotNull();
        then(config.getMaxFragmentNumber()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT);
        then(config.getMaxFragmentSize()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT);
        then(config.getEndpointProtocol()).isEqualTo("https");
        then(config.getEndpoint()).isEmpty();
        then(config.getBucketName()).isNull();
        // Region is null only when neither aws.region system property nor AWS_REGION env var is set.
        // The build sets -Daws.region for the test JVM (and the shell may export AWS_REGION), so
        // clear both to assert the unset invariant.
        restoreSystemProperties(() -> {
            System.clearProperty(AWS_REGION_PROPERTY);
            withEnvironmentVariable("AWS_REGION", null).execute(() ->
                then(new S3NioSpiConfiguration().getRegion()).isNull());
        });
        then(config.getCredentials()).isNull();
        then(config.getCredentialsProvider()).isNull();
        then(config.getForcePathStyle()).isFalse();
        then(config.getTimeoutLow()).isEqualTo(S3_SPI_TIMEOUT_LOW_DEFAULT);
        then(config.getTimeoutMedium()).isEqualTo(S3_SPI_TIMEOUT_MEDIUM_DEFAULT);
        then(config.getTimeoutHigh()).isEqualTo(S3_SPI_TIMEOUT_HIGH_DEFAULT);
        then(config.getIntegrityCheckAlgorithm()).isEqualTo(S3_INTEGRITY_CHECK_ALGORITHM_DEFAULT);
    }

    @Test
    @DisplayName("new S3NioSpiConfiguration should not support `null` Map of overrides")
    public void nullMapNotSupported() {
        assertThrows(NullPointerException.class, () -> new S3NioSpiConfiguration((Map<String, ?>) null));
    }

    @Test
    public void overridesAsMap() {
        Map<String, String> map = new HashMap<>();
        map.put(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, "1212");
        var c = new S3NioSpiConfiguration(map);

        then(c.getMaxFragmentSize()).isEqualTo(1212);
    }

    @Test
    public void getS3SpiReadMaxFragmentSize() {
        then(config.getMaxFragmentSize()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT);

        then(overriddenConfig.getMaxFragmentSize()).isEqualTo(1111);
        then(badOverriddenConfig.getMaxFragmentSize()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT);
    }

    @Test
    public void getS3SpiReadMaxFragmentNumber() {
        then(config.getMaxFragmentNumber()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT);

        then(overriddenConfig.getMaxFragmentNumber()).isEqualTo(2);
        then(badOverriddenConfig.getMaxFragmentNumber()).isEqualTo(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT);
    }

    @Test
    public void withAndGetRegion() throws Exception {
        // With no aws.region system property or AWS_REGION env var, region is null (the build sets
        // -Daws.region for the test JVM and the shell may export AWS_REGION, so clear both to assert
        // the unset invariant).
        restoreSystemProperties(() -> {
            System.clearProperty(AWS_REGION_PROPERTY);
            withEnvironmentVariable("AWS_REGION", null).execute(() ->
                then(new S3NioSpiConfiguration().getRegion()).isNull());
        });

        var env = new Properties();
        env.setProperty(AWS_REGION_PROPERTY, "region1");

        final var C = new S3NioSpiConfiguration(env);
        then(C.getRegion()).isEqualTo("region1");
        then(C.withRegion("\tregion2 ")).isSameAs(C);
        then(C.getRegion()).isEqualTo("region2");
        then(C.withRegion(" \t ").getRegion()).isNull();
        then(C.withRegion("").getRegion()).isNull();
        then(C.withRegion(null).getRegion()).isNull();
    }

    @Test
    public void regionReadFromEnvVar() throws Exception {
        restoreSystemProperties(() -> {
            System.clearProperty(AWS_REGION_PROPERTY);
            withEnvironmentVariable("AWS_REGION", "eu-west-1").execute(() ->
                then(new S3NioSpiConfiguration().getRegion()).isEqualTo("eu-west-1"));
        });
    }

    @Test
    public void regionReadFromSystemProperty() throws Exception {
        restoreSystemProperties(() -> {
            System.setProperty(AWS_REGION_PROPERTY, "ap-south-1");
            then(new S3NioSpiConfiguration().getRegion()).isEqualTo("ap-south-1");
        });
    }

    @Test
    public void systemPropertyRegionOverridesEnvRegion() throws Exception {
        restoreSystemProperties(() -> {
            withEnvironmentVariable("AWS_REGION", "eu-west-1").execute(() -> {
                System.setProperty(AWS_REGION_PROPERTY, "ap-south-1");
                then(new S3NioSpiConfiguration().getRegion()).isEqualTo("ap-south-1");
            });
        });
    }

    @Test
    public void endpointReadFromEnvAndSystemProperty() throws Exception {
        withEnvironmentVariable("S3_SPI_ENDPOINT", "endpoint-from-env.example.com:9000").execute(() ->
            then(new S3NioSpiConfiguration().getEndpoint()).isEqualTo("endpoint-from-env.example.com:9000"));

        restoreSystemProperties(() -> {
            System.setProperty(S3_SPI_ENDPOINT_PROPERTY, "endpoint-from-sysprop.example.com:9001");
            then(new S3NioSpiConfiguration().getEndpoint()).isEqualTo("endpoint-from-sysprop.example.com:9001");
        });
    }

    @Test
    public void credentialsNotReadFromEnvironment() throws Exception {
        // The AWS SDK default credential provider chain handles aws.accessKeyId / aws.secretAccessKey;
        // this library deliberately does not read them into its configuration.
        restoreSystemProperties(() -> {
            System.clearProperty(AWS_ACCESS_KEY_PROPERTY);
            System.clearProperty(AWS_SECRET_ACCESS_KEY_PROPERTY);
            withEnvironmentVariable("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
                .and("AWS_SECRET_ACCESS_KEY", "secretexample")
                .execute(() -> then(new S3NioSpiConfiguration().getCredentials()).isNull());
        });
    }

    @Test
    public void withOverridesAppliesEnvMapAtHighestPrecedence() {
        var credentials = AwsBasicCredentials.create("k", "s");
        var overrides = Map.of(
            AWS_REGION_PROPERTY, "sa-east-1",
            S3_SPI_TIMEOUT_LOW_PROPERTY, "9",
            S3_SPI_CREDENTIALS_PROPERTY, credentials);

        then(config.withOverrides(overrides)).isSameAs(config);
        then(config.getRegion()).isEqualTo("sa-east-1");
        then(config.getTimeoutLow()).isEqualTo(9L);
        // typed objects pass through untouched
        then(config.getCredentials()).isSameAs(credentials);

        // null is a no-op
        then(config.withOverrides(null)).isSameAs(config);
    }

    @Test
    public void asMapIsUnmodifiableSnapshot() {
        var snapshot = config.asMap();
        assertThrows(UnsupportedOperationException.class, () -> snapshot.put("x", "y"));

        // mutating the config afterwards does not affect the earlier snapshot
        config.withTimeoutLow(42L);
        then(snapshot).doesNotContainEntry(S3_SPI_TIMEOUT_LOW_PROPERTY, "42");
        then(config.asMap()).containsEntry(S3_SPI_TIMEOUT_LOW_PROPERTY, "42");
    }

    @Test
    public void withAndGetEndpoint() {
        then(config.withEndpoint("somewhere.com:8000")).isSameAs(config);
        then(config.getEndpoint()).isEqualTo("somewhere.com:8000");
        then(config.withEndpoint(" somewhere.com:8080\t").getEndpoint()).isEqualTo("somewhere.com:8080");
        then(config.withEndpoint("   ").getEndpoint()).isEqualTo("");
        then(config.withEndpoint(null).getEndpoint()).isEqualTo("");
        then(config.withEndpoint("noport.somewhere.com").getEndpoint()).isEqualTo("noport.somewhere.com");

        assertThatCode(() -> config.withEndpoint("wrongport.somewhere.com:aabbcc"))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("endpoint 'wrongport.somewhere.com:aabbcc' does not match format host:port where port is a number");
    }

    @Test
    public void withAndGetEndpointProtocol() {
        then(config.withEndpointProtocol("http")).isSameAs(config);
        then(config.getEndpointProtocol()).isEqualTo("http");
        then(config.withEndpointProtocol("  http\n").getEndpointProtocol()).isEqualTo("http");
        then(overriddenConfig.getEndpointProtocol()).isEqualTo("https");
        then(badOverriddenConfig.getEndpointProtocol()).isEqualTo(S3_SPI_ENDPOINT_PROTOCOL_DEFAULT);

        assertThatCode(() -> config.withEndpointProtocol("ftp"))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("endpoint prococol must be one of ('http', 'https')");
    }

    @Test
    public void withAndGetMaxFragmentNumber() {
        then(config.withMaxFragmentNumber(1000)).isSameAs(config);
        then(config.getMaxFragmentNumber()).isEqualTo(1000);

        assertThatCode(() -> config.withMaxFragmentNumber(-1))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxFragmentNumber must be positive");
    }

    @Test
    public void withAndGetMaxFragmentSize() {
        then(config.withMaxFragmentSize(4000)).isSameAs(config);
        then(config.getMaxFragmentSize()).isEqualTo(4000);

        assertThatCode(() -> config.withMaxFragmentSize(-1))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxFragmentSize must be positive");
    }

    @Test
    public void withAndGetPlainCredentials() {
        then(config.withCredentials("akey", "asecret")).isSameAs(config);

        var credentials = config.getCredentials();
        then(credentials.accessKeyId()).isEqualTo("akey");
        then(credentials.secretAccessKey()).isEqualTo("asecret");

        credentials = config.withCredentials("anotherkey", "anothersecret").getCredentials();
        then(credentials.accessKeyId()).isEqualTo("anotherkey");
        then(credentials.secretAccessKey()).isEqualTo("anothersecret");

        then(config.withCredentials(null, "something").getCredentials()).isNull();

        assertThatCode(() -> config.withCredentials("akey", null))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("secretAccessKey can not be null");
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
        final AwsCredentials C1 = AwsBasicCredentials.create("key1", "secret1");
        final AwsCredentials C2 = AwsBasicCredentials.create("key2", "secret2");

        then(config.withCredentials(C1)).isSameAs(config);
        then(config.getCredentials()).isSameAs(C1);
        then(config.withCredentials(C2)).isSameAs(config);
        then(config.getCredentials()).isSameAs(C2);
        then(config.withCredentials(null).getCredentials()).isNull();
        then(config.withCredentials(C1).withCredentials(null, null).getCredentials()).isNull();

        //
        // withCredentials(AwsCredentials) takes priority over withCredentialas(String, String)
        //
        then(
            config.withCredentials(C1.accessKeyId(), C2.secretAccessKey())
            .withCredentials(C2)
            .getCredentials()
        ).isSameAs(C2);
    }

    @Test
    public void withAndGetCredentialsProvider() {

        final AwsCredentialsProvider C1 = () -> AwsBasicCredentials.create("key1", "secret1");
        final AwsCredentialsProvider C2 = () -> AwsBasicCredentials.create("key2", "secret2");

        then(config.withCredentialsProvider(C1)).isSameAs(config);
        then(config.getCredentialsProvider()).isSameAs(C1);
        then(config.withCredentialsProvider(C2)).isSameAs(config);
        then(config.getCredentialsProvider()).isSameAs(C2);
        then(config.withCredentialsProvider(C1).withCredentialsProvider(null).getCredentialsProvider()).isNull();

        //
        // withCredentialsProvider(AwsCredentialsProvider) takes priority over withCredentialas
        //
        then(
                config.withCredentials("key1", "secret1")
                        .withCredentialsProvider(C2)
                        .getCredentialsProvider()
        ).isSameAs(C2);
    }

    @Test
    public void getCredentialsProviderWithCredentials() {

        final AwsCredentials C1 = AwsBasicCredentials.create("key1", "secret1");

        then(config.withCredentials(C1)).isSameAs(config);
        then(config.getCredentials()).isSameAs(C1);
        then(config.withCredentials(null).getCredentials()).isNull();
        then(config.withCredentials(null).withCredentialsProvider(null).getCredentialsProvider()).isNull();

        then(
                config.withCredentials(C1)
                        .withCredentialsProvider(null)
                        .getCredentials()
        ).isSameAs(C1);
    }

    @Test
    public void getCredentialsProviderWithWrongTypeOfObject() {

        final AwsCredentials C1 = AwsBasicCredentials.create("key1", "secret1");
        config.withOverrides(Map.of(S3_SPI_CREDENTIALS_PROVIDER_PROPERTY, "IAmNotAProvider"));
        then(config.getCredentialsProvider()).isNull();

        then(config.withCredentials(C1)).isSameAs(config);
        then(config.getCredentials()).isSameAs(C1);
        then(
                config.withCredentials(C1)
                        .withCredentialsProvider(null)
                        .getCredentials()
        ).isSameAs(C1);
    }

    @Test
    public void convertPropertyNameToEnvVar() {
        var expected = "FOO_BAA_FIZZ_BUZZ";
        then(config.convertPropertyNameToEnvVar("foo.baa.fizz-buzz")).isEqualTo(expected);

        expected = "";
        then(config.convertPropertyNameToEnvVar(null)).isEqualTo(expected);
        then(config.convertPropertyNameToEnvVar("  ")).isEqualTo(expected);
    }

    @Test
    public void withAndGetBucketName() {
        then(config.withBucketName("aname")).isSameAs(config);
        then(config.getBucketName()).isEqualTo("aname");
        then(config.withBucketName("anothername").getBucketName()).isEqualTo("anothername");
        then(config.withBucketName(null).getBucketName()).isNull();

        assertThatCode(() -> config.withBucketName("Wrong/bucket;name"))
                .as("missing sanity check")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket name should not contain uppercase characters");
    }
    
    @Test
    public void withAndGetForcePathStyle() {
        then(config.asMap()).containsEntry(S3_SPI_FORCE_PATH_STYLE_PROPERTY, "false");
        then(config.withForcePathStyle(true)).isSameAs(config);
        then(config.asMap()).containsEntry(S3_SPI_FORCE_PATH_STYLE_PROPERTY, "true");
        then(config.getForcePathStyle()).isTrue();
        then(config.withForcePathStyle(false).getForcePathStyle()).isFalse();

        Map<String, Object> map = new HashMap<>(); config = new S3NioSpiConfiguration(map);
        then(config.getForcePathStyle()).isFalse();
        map.put(S3_SPI_FORCE_PATH_STYLE_PROPERTY, "true"); config = new S3NioSpiConfiguration(map);
        then(config .getForcePathStyle()).isTrue();
        map.remove(S3_SPI_FORCE_PATH_STYLE_PROPERTY); // same S3NioSpiConfiguration on purpose
        then(config.getForcePathStyle()).isTrue();
        then(config.withForcePathStyle(null).getForcePathStyle()).isFalse();
        then(config.asMap()).doesNotContainKey(S3_SPI_FORCE_PATH_STYLE_PROPERTY);
    }

    @Test
    public void withAndGetTimeoutLow() {
        then(config.asMap()).containsEntry(S3_SPI_TIMEOUT_LOW_PROPERTY, "1");
        then(config.withTimeoutLow(4L)).isSameAs(config);
        then(config.asMap()).containsEntry(S3_SPI_TIMEOUT_LOW_PROPERTY, "4");
        then(config.getTimeoutLow()).isEqualTo(4L);
        then(config.withTimeoutLow(5L).getTimeoutLow()).isEqualTo(5L);
    }

    @Test
    public void withAndGetTimeoutMedium() {
        then(config.asMap()).containsEntry(S3_SPI_TIMEOUT_MEDIUM_PROPERTY, "3");
        then(config.withTimeoutMedium(5L)).isSameAs(config);
        then(config.asMap()).containsEntry(S3_SPI_TIMEOUT_MEDIUM_PROPERTY, "5");
        then(config.getTimeoutMedium()).isEqualTo(5L);
        then(config.withTimeoutMedium(6L).getTimeoutMedium()).isEqualTo(6L);
    }

    @Test
    public void withAndGetTimeoutHigh() {
        then(config.asMap()).containsEntry(S3_SPI_TIMEOUT_HIGH_PROPERTY, "5");
        then(config.withTimeoutHigh(7L)).isSameAs(config);
        then(config.asMap()).containsEntry(S3_SPI_TIMEOUT_HIGH_PROPERTY, "7");
        then(config.getTimeoutHigh()).isEqualTo(7L);
        then(config.withTimeoutHigh(8L).getTimeoutHigh()).isEqualTo(8L);
    }

    @Test
    public void withAndGetIntegrityCheckAlgorithm() throws Exception {
        then(config.asMap()).containsEntry(S3_INTEGRITY_CHECK_ALGORITHM_PROPERTY, "disabled");
        then(config.withIntegrityCheckAlgorithm("CRC32C")).isSameAs(config);
        then(config.asMap()).containsEntry(S3_INTEGRITY_CHECK_ALGORITHM_PROPERTY, "CRC32C");
        then(config.getIntegrityCheckAlgorithm()).isEqualTo("CRC32C");
        then(config.withIntegrityCheckAlgorithm("CRC64NVME").getIntegrityCheckAlgorithm()).isEqualTo("CRC64NVME");

        var map = new HashMap<String, String>();
        map.put(S3_INTEGRITY_CHECK_ALGORITHM_PROPERTY, "invalid");
        var c = new S3NioSpiConfiguration(map);

        thenExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> c.getIntegrityCheckAlgorithm())
            .withMessage("unknown integrity check algorithm 'invalid'");

        thenExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> c.withIntegrityCheckAlgorithm("unknown algorithm"))
            .withMessage("unknown integrity check algorithm 'unknown algorithm'");

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC32C")
            .execute(() -> then(new S3NioSpiConfiguration().getIntegrityCheckAlgorithm()).isEqualTo("CRC32C"));

        withEnvironmentVariable("S3_INTEGRITY_CHECK_ALGORITHM", "CRC64NVME")
            .execute(() -> then(new S3NioSpiConfiguration().getIntegrityCheckAlgorithm()).isEqualTo("CRC64NVME"));

        then(new S3NioSpiConfiguration(Map.of(S3_INTEGRITY_CHECK_ALGORITHM_PROPERTY, "CRC32")).getIntegrityCheckAlgorithm()).isEqualTo("CRC32");
    }

    @Test
    public void withAndGetOpenOptions() {
        // by default `useTransferManager` is set
        then(config.asMap()).containsEntry(S3_OPEN_OPTIONS_PROPERTY, Set.of(S3OpenOption.useTransferManager()));
        then(config.getOpenOptions()).containsExactly(S3OpenOption.useTransferManager());

        // clear all default open options
        then(config.withOpenOptions(Set.of())).isSameAs(config);
        then(config.asMap()).containsEntry(S3_OPEN_OPTIONS_PROPERTY, Set.of());
        then(config.getOpenOptions()).isEmpty();

        // set `preventConcurrentOverwrite`
        var option1 = S3OpenOption.preventConcurrentOverwrite();
        var option2 = S3OpenOption.putOnlyIfModified();
        var newOptions = Set.of(option1, option2);
        then(config.withOpenOptions(newOptions)).isSameAs(config);
        then(config.getOpenOptions()).hasSize(2);
        config.getOpenOptions().forEach(o -> {
            then(o).isInstanceOfAny(option1.getClass(), option2.getClass());
            then(o).isNotInstanceOf(S3OpenOption.useTransferManager().getClass());
        });
    }

    @Test
    public void withAndGetOpenOptions_duplicateCheck() {
        thenThrownBy(() -> config.withOpenOptions(List.of(S3OpenOption.useTransferManager(), S3OpenOption.useTransferManager())))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("Duplicate key software.amazon.nio.spi.s3.S3UseTransferManager");
        thenThrownBy(() -> config.withOpenOptions(List.of(S3OpenOption.preventConcurrentOverwrite(), S3OpenOption.preventConcurrentOverwrite())))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("Duplicate key software.amazon.nio.spi.s3.S3PreventConcurrentOverwrite");
    }

    @Test
    public void streamingMultipartUploadDefaultValues() {
        then(config.isStreamingMultipartUploadEnabled()).isFalse();
        then(config.getMultipartPartSize()).isEqualTo(S3_SPI_WRITE_MULTIPART_PART_SIZE_DEFAULT);
        then(config.getMultipartPartSize()).isEqualTo(8L * 1024 * 1024);
    }

    @Test
    public void streamingMultipartUploadEnvVarOverride() throws Exception {
        withEnvironmentVariable("S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD", "true")
            .and("S3_SPI_WRITE_MULTIPART_PART_SIZE", "16777216")
            .execute(() -> {
                var c = new S3NioSpiConfiguration();
                then(c.isStreamingMultipartUploadEnabled()).isTrue();
                then(c.getMultipartPartSize()).isEqualTo(16777216L);
            });
    }

    @Test
    public void streamingMultipartUploadSystemPropertyOverride() throws Exception {
        withEnvironmentVariable("S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD", "false")
            .execute(() -> {
                restoreSystemProperties(() -> {
                    System.setProperty(S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD_PROPERTY, "true");
                    System.setProperty(S3_SPI_WRITE_MULTIPART_PART_SIZE_PROPERTY, "10485760");
                    var c = new S3NioSpiConfiguration();
                    then(c.isStreamingMultipartUploadEnabled()).isTrue();
                    then(c.getMultipartPartSize()).isEqualTo(10485760L);
                });
            });
    }

    @Test
    public void streamingMultipartUploadProgrammaticOverride() {
        Map<String, String> overridesMap = new HashMap<>();
        overridesMap.put(S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD_PROPERTY, "true");
        overridesMap.put(S3_SPI_WRITE_MULTIPART_PART_SIZE_PROPERTY, "20971520");
        var c = new S3NioSpiConfiguration(overridesMap);
        then(c.isStreamingMultipartUploadEnabled()).isTrue();
        then(c.getMultipartPartSize()).isEqualTo(20971520L);
    }

    @Test
    public void getOpenOptionsIncludesStreamingWhenEnabled() {
        config.withStreamingMultipartUpload(true);
        var options = config.getOpenOptions();
        then(options).anyMatch(o -> o.getClass().getName().contains("S3StreamingMultipartUpload"));
    }

    @Test
    public void getOpenOptionsExcludesStreamingWhenDisabled() {
        config.withStreamingMultipartUpload(false);
        var options = config.getOpenOptions();
        then(options).noneMatch(o -> o.getClass().getName().contains("S3StreamingMultipartUpload"));
    }

    @Test
    public void invalidPartSizeLogsWarningAndUsesDefault() {
        config.withOverrides(Map.of(S3_SPI_WRITE_MULTIPART_PART_SIZE_PROPERTY, "not-a-number"));
        then(config.getMultipartPartSize()).isEqualTo(S3_SPI_WRITE_MULTIPART_PART_SIZE_DEFAULT);
    }

    @Test
    public void withStreamingMultipartUploadFluentSetter() {
        then(config.withStreamingMultipartUpload(true)).isSameAs(config);
        then(config.isStreamingMultipartUploadEnabled()).isTrue();
        then(config.withStreamingMultipartUpload(false)).isSameAs(config);
        then(config.isStreamingMultipartUploadEnabled()).isFalse();
    }

    @Test
    public void withMultipartPartSizeFluentSetter() {
        long validSize = 10 * 1024 * 1024L; // 10 MiB
        then(config.withMultipartPartSize(validSize)).isSameAs(config);
        then(config.getMultipartPartSize()).isEqualTo(validSize);
    }

    @Test
    public void withMultipartPartSizeValidation() {
        long tooSmall = 4 * 1024 * 1024L; // 4 MiB (below 5 MiB minimum)
        assertThatCode(() -> config.withMultipartPartSize(tooSmall))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("partSize must be at least 5 MiB");

        long tooLarge = 6L * 1024 * 1024 * 1024; // 6 GiB (above 5 GiB maximum)
        assertThatCode(() -> config.withMultipartPartSize(tooLarge))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("partSize must not exceed 5 GiB");
    }

}
