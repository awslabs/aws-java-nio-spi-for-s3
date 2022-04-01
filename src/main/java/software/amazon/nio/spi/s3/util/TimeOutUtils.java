/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import org.slf4j.Logger;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class TimeOutUtils {

    public static long TIMEOUT_TIME_LENGTH_1 = 1L;
    public static long TIMEOUT_TIME_LENGTH_3 = 3L;
    public static long TIMEOUT_TIME_LENGTH_5 = 5L;

    public static String createTimeOutMessage(String operationName, long length, TimeUnit unit) {
        return format("the %s operation timed out after %d %s, check your network connectivity and status of S3 service",
                  operationName, length, unit.toString().toLowerCase(Locale.ROOT));
    }

    public static String createAndLogTimeOutMessage(Logger logger, String operationName, long length, TimeUnit unit) {
        Objects.requireNonNull(logger);
        String msg = createTimeOutMessage(operationName, length, unit);
        logger.error(msg);
        return msg;
    }

    public static RuntimeException logAndGenerateExceptionOnTimeOut(Logger logger, String operationName, long length, TimeUnit unit){
        Objects.requireNonNull(logger);
        String msg = createAndLogTimeOutMessage(logger, operationName, length, unit);
        return new RuntimeException(msg);
    }
}
