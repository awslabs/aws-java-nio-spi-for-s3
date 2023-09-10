/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3.util;

import java.util.StringJoiner;

import software.amazon.awssdk.services.s3.internal.BucketUtils;

/**
 *
 */
public class StringUtils {
    /**
     * Join method to join multiple elements starting from the element with
     * index {@code startAt}. Elements are joined with the provided
     * {@code delimiter} in between them. Elements with index less then
     * {@code startAt} are ignored and will are not added to the returned
     * string.
     *
     * @param separator the delimiter that separates each element
     * @param elements the substrings to join
     * @param startAt the index of the element in {@code elements} to start joining
     *
     * @return a new {@code String} that is composed of the {@code elements}
     *         separated by the {@code delimiter}
     */
    public static String joinFrom(String separator, String[] elements, int startAt) {
        StringJoiner sj = new StringJoiner(separator);

        for(; startAt < elements.length; ++startAt) {
            sj.add(elements[startAt]);
        }

        return sj.toString();
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
        for (int i=0; i<s.length(); ++i) {
            if (!Character.isWhitespace(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}