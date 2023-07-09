/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;


import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Pair;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

/**
 * Object to hold configuration of the S3 NIO SPI
 */
public class S3NioSpiConfiguration extends HashMap<String, String> {

    public static final String AWS_REGION_PROPERTY = "aws.region";
    public static final String AWS_ACCESS_KEY_PROPERTY = "aws.accessKey";
    public static final String AWS_SECRET_ACCESS_KEY_PROPERTY = "aws.secretAccessKey";

    /**
     * The name of the maximum fragment size property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY = "s3.spi.read.max-fragment-size";
    /**
     * The default value of the maximum fragment size property
     */
    public static final int S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT = 5242880;
    /**
     * The name of the maximum fragment number property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY = "s3.spi.read.max-fragment-number";
    /**
     * The default value of the maximum fragment size property
     */
    public static final int S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT = 50;
    /**
     * The name of the endpoint property
     */
    public static final String S3_SPI_ENDPOINT_PROPERTY = "s3.spi.endpoint";
    /**
     * The default value of the endpoint property
     */
    public static final String S3_SPI_ENDPOINT_DEFAULT = "";
    /**
     * The name of the endpoint protocol property
     */
    public static final String S3_SPI_ENDPOINT_PROTOCOL_PROPERTY = "s3.spi.endpoint-protocol";
    /**
     * The default value of the endpoint protocol property
     */
    public static final String S3_SPI_ENDPOINT_PROTOCOL_DEFAULT = "https";

    private final Pattern ENDPOINT_REGEXP = Pattern.compile("(\\w[\\w\\-\\.]*)(?::(\\d+))");
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Create a new, empty configuration
     */
    public S3NioSpiConfiguration(){
        this(new HashMap<>());
    }

    /**
     * Create a new, empty configuration
     */
    public S3NioSpiConfiguration(Map<String, ?> overrides) {
        Objects.requireNonNull(overrides);

        //
        // setup defaults
        //
        put(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, String .valueOf(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT));
        put(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, String .valueOf(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT));

        //
        // With the below we pick existing environment variables and system
        // properties as overrides of the default aws-nio specific properties.
        // We do not pick aws generic properties like aws.region or
        // aws.accessKey, leaving the framework and the underlying AWS client
        // the possibility to use the standard behaviour.
        //
        // TOTO: shall we return an Optional instead returning null?
        // TOTO: shall we return an Optional instead returning null?
        //

        //add env var overrides if present
        keySet().stream()
                .map(key -> Pair.of(key,
                        Optional.ofNullable(System.getenv().get(this.convertPropertyNameToEnvVar(key)))))
                .forEach(pair -> pair.right().ifPresent(val -> put(pair.left(), val)));

        //add System props as overrides if present
        keySet().forEach(
            key -> Optional.ofNullable(System.getProperty(key)).ifPresent(val -> put(key, val))
        );

        overrides.keySet().forEach(key -> put(key, String.valueOf(overrides.get(key))));
    }

    /**
     * Create a new, empty configuration
     */
    public S3NioSpiConfiguration(Map<String, ?> overrides){
        Objects.requireNonNull(overrides);
        overrides.keySet()
            .forEach(key -> properties.setProperty(key, String.valueOf(overrides.get(key))));
    }

    /**
     * Create a new configuration with overrides
     * @param overrides the overrides
     */
    //
    // TODO: to be removed if not used
    //
    protected S3NioSpiConfiguration(Properties overrides) {
        Objects.requireNonNull(overrides);
        overrides.stringPropertyNames()
            .forEach(key -> put(key, overrides.getProperty(key)));
    }

    /**
     * Fluently sets the value of maximum fragment number
     *
     * @param maxFragmentNumber the maximum fragment number
     *
     * @return this instance
     */
    public S3NioSpiConfiguration withMaxFragmentNumber(int maxFragmentNumber) {
        if (maxFragmentNumber < 1) {
            throw new IllegalArgumentException("maxFragmentNumber must be positive");
        }
        put(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, String.valueOf(maxFragmentNumber));
        return this;
    }

    /**
     * Fluently sets the value of maximum fragment size
     *
     * @param maxFragmentSize the maximum fragment size
     *
     * @return this instance
     */
    public S3NioSpiConfiguration withMaxFragmentSize(int maxFragmentSize) {
        if (maxFragmentSize < 1) {
            throw new IllegalArgumentException("maxFragmentSize must be positive");
        }
        put(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, String.valueOf(maxFragmentSize));
        return this;
    }

    /**
     * Fluently sets the value of the endpoint
     *
     * @param endpoint the endpoint
     *
     * @return this instance
     */
    public S3NioSpiConfiguration withEndpoint(String endpoint) {

        if (endpoint == null) {
            endpoint = "";
        }
        endpoint = endpoint.trim();

        if ((endpoint.length() > 0) && !ENDPOINT_REGEXP.matcher(endpoint).find()) {
            throw new IllegalArgumentException(
                String.format("endpoint '%s' does not match format host:port", endpoint)
            );
        }

        put(S3_SPI_ENDPOINT_PROPERTY, endpoint); return this;
    }

    /**
     * Fluently sets the value of the endpoint's protocol
     *
     * @param protocol the endpoint's protcol
     *
     * @return this instance
     */
    public S3NioSpiConfiguration withEndpointProtocol(String protocol) {
        if (protocol != null) {
            protocol = protocol.trim();
        }
        if (!"http".equals(protocol) && !"https".equals(protocol)) {
            throw new IllegalArgumentException("endpoint prococol must be one of ('http', 'https')");
        }
        put(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, protocol); return this;
    }

    /**
     * Get the value of the Maximum Fragment Size
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentSize(){
        return parseIntProperty(
            S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY,
            S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT
        );
    }

    /**
     * Get the value of the Maximum Fragment Number
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentNumber(){
        return parseIntProperty(
            S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY,
            S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT
        );
    }

    /**
     * Get the value of the endpoint. Not that no endvar/sysprop is taken as
     * default.
     *
     * @return the configured value or the default ("") if not overridden
     */
    public String getEndpoint() {
        return getOrDefault(S3_SPI_ENDPOINT_PROPERTY, S3_SPI_ENDPOINT_DEFAULT);
    }

    /**
     * Get the value of the endpoint protocol
     * @return the configured value or the default if not overridden
     */
    public String getEndpointProtocol() {
        String protocol = getOrDefault(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, S3_SPI_ENDPOINT_PROTOCOL_DEFAULT);
        if ("http".equalsIgnoreCase(protocol) || "https".equalsIgnoreCase(protocol)) {
            return protocol;
        }
        logger.warn("the value of '{}' for '{}' is not 'http'|'https', using default value of '{}'",
                    protocol, S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, S3_SPI_ENDPOINT_PROTOCOL_DEFAULT);
        return S3_SPI_ENDPOINT_PROTOCOL_DEFAULT;
    }

    /**
     * Get the configured credentials
     * @return the configured value or null if not provided
     */
    public AwsCredentials getCredentials() {
        if (containsKey(AWS_ACCESS_KEY_PROPERTY)) {
            return AwsBasicCredentials.create(
                get(AWS_ACCESS_KEY_PROPERTY),
                get(AWS_SECRET_ACCESS_KEY_PROPERTY)
           );
        }

        return null;
    }

    /**
     * Get the configured region if any
     *
     * @return the configured value or null if not provided
     */
    public String getRegion() {
        return get(AWS_REGION_PROPERTY);
    }

    // ------------------------------------------------------- protected methods

    /**
     * Generates an environment variable name from a property name. E.g 'some.property' becomes 'SOME_PROPERTY'
     * @param propertyName the name to convert
     * @return the converted name
     */
    protected String convertPropertyNameToEnvVar(String propertyName){
        if(propertyName == null || propertyName.trim().isEmpty()) return "";

        return propertyName
                .trim()
                .replace('.', '_').replace('-', '_')
                .toUpperCase(Locale.ROOT);
    }

    // --------------------------------------------------------- private methods

    private int parseIntProperty(String propName, int defaultVal){
        String propertyVal = get(propName);
        try{
            return Integer.parseInt(propertyVal);
        } catch (NumberFormatException e){
            logger.warn("the value of '{}' for '{}' is not an integer, using default value of '{}'",
                    propertyVal, propName, defaultVal);
            return defaultVal;
        }
    }
}
