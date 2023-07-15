/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3.util;

import software.amazon.nio.spi.s3.S3Path;

/**
 *
 */
public class StringUtils {
    public static String join(String separator, String[] elements, int startAt) {
        StringBuilder sb = new StringBuilder();

        for(; startAt < elements.length; ++startAt) {
            sb.append(S3Path.PATH_SEPARATOR).append(elements[startAt]);
        }

        return sb.toString();
    }

    /**
     * Like Java 11's String.isBlank(): it returns true if the string is empty or 
     * contains only white space codepoints, otherwise false.
     * 
     * @param s the string to check
     * 
     * @return true if the string is empty or contains only white space codepoints, otherwise false
     * 
     * TODO: to replace with Java11's isBlank when moving away from Java8
     */
    public static boolean isBlank(final String s) {
        return s.replace(" ", "").replace("\n", "").replace("\r", "").replace("\t", "").isEmpty();
    }
}