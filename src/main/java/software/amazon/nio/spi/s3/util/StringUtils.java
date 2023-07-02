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
}
