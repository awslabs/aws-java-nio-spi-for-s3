/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Pair;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * Object to hold configuration of the S3 NIO SPI
 */
public class S3NioSpiConfiguration {

    /**
     * The name of the maximum fragment size property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY = "s3.spi.read.max-fragment-size";
    /**
     * The default value of the maximum fragment size property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT = "5242880";
    /**
     * The name of the maximum fragment number property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY = "s3.spi.read.max-fragment-number";
    /**
     * The default value of the maximum fragment size property
     */
    public static final String S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT = "50";

    private final Properties properties;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    {
        final Properties defaults = new Properties();
        defaults.put(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY, S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT);
        defaults.put(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY, S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT);

        //setup defaults
        properties = new Properties(defaults);

        //add env var overrides if present
        properties.stringPropertyNames().stream()
                .map(key -> Pair.of(key,
                        Optional.ofNullable(System.getenv().get(this.convertPropertyNameToEnvVar(key)))))
                .forEach(pair -> pair.right().ifPresent(val -> properties.setProperty(pair.left(), val)));

        //add System props as overrides if present
        properties.stringPropertyNames()
                .forEach(key -> Optional.ofNullable(System.getProperty(key))
                        .ifPresent(val -> properties.put(key, val)));

    }

    /**
     * Create a new, empty configuration
     */
    public S3NioSpiConfiguration(){
        this(new Properties());
    }

    /**
     * Create a new configuration with overrides
     * @param overrides the overrides
     */
    protected S3NioSpiConfiguration(Properties overrides) {
        Objects.requireNonNull(overrides);
        overrides.stringPropertyNames()
                .forEach(key -> properties.setProperty(key, overrides.getProperty(key)));
    }

    /**
     * Get the value of the Maximum Fragment Size
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentSize(){
        return parseIntProperty(S3_SPI_READ_MAX_FRAGMENT_SIZE_PROPERTY,
                Integer.parseInt(S3_SPI_READ_MAX_FRAGMENT_SIZE_DEFAULT));
    }

    /**
     * Get the value of the Maximum Fragment Number
     * @return the configured value or the default if not overridden
     */
    public int getMaxFragmentNumber(){
        return parseIntProperty(S3_SPI_READ_MAX_FRAGMENT_NUMBER_PROPERTY,
                Integer.parseInt(S3_SPI_READ_MAX_FRAGMENT_NUMBER_DEFAULT));
    }

    private int parseIntProperty(String propName, int defaultVal){
        String propertyVal = properties.getProperty(propName);
        try{
            return Integer.parseInt(propertyVal);
        } catch (NumberFormatException e){
            logger.warn("the value of '{}' for '{}' is not an integer, using default value of '{}'",
                    propertyVal, propName, defaultVal);
            return defaultVal;
        }
    }

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
}
