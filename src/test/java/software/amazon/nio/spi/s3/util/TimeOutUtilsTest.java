/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3.util;

import org.mockito.Mockito;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TimeOutUtilsTest {

    @Spy  //because Logger is final we use MockMaker inline extension (setting in src/test/resources/mockito-extensions/org.mockito.plugins.MockMaker)
    Logger spyLogger = LoggerFactory.getLogger(this.getClass().toString());

    @Test
    public void createTimeOutMessage() {
        var timeOutMessage = TimeOutUtils.createTimeOutMessage("foo", TimeOutUtils.TIMEOUT_TIME_LENGTH_1, TimeUnit.MINUTES);
        assertEquals("the foo operation timed out after 1 minutes, check your network connectivity and status of S3 service", timeOutMessage);

        timeOutMessage = TimeOutUtils.createTimeOutMessage("foo", TimeOutUtils.TIMEOUT_TIME_LENGTH_3, TimeUnit.MINUTES);
        assertEquals("the foo operation timed out after 3 minutes, check your network connectivity and status of S3 service", timeOutMessage);

        timeOutMessage = TimeOutUtils.createTimeOutMessage("foo", TimeOutUtils.TIMEOUT_TIME_LENGTH_5, TimeUnit.MINUTES);
        assertEquals("the foo operation timed out after 5 minutes, check your network connectivity and status of S3 service", timeOutMessage);

    }

    @Test
    public void createAndLogTimeOutMessage() {
        var timeOutMessage = TimeOutUtils.createAndLogTimeOutMessage(spyLogger, "foo", TimeOutUtils.TIMEOUT_TIME_LENGTH_1, TimeUnit.MINUTES);
        assertEquals("the foo operation timed out after 1 minutes, check your network connectivity and status of S3 service", timeOutMessage);
        Mockito.verify(spyLogger).error(timeOutMessage);
    }

    @Test
    public void logAndGenerateExceptionOnTimeOut() {
        final var msg = "the foo operation timed out after 1 minutes, check your network connectivity and status of S3 service";
        var timeOut = TimeOutUtils.logAndGenerateExceptionOnTimeOut(spyLogger, "foo", TimeOutUtils.TIMEOUT_TIME_LENGTH_1, TimeUnit.MINUTES);
        assertEquals(msg, timeOut.getMessage());
        Mockito.verify(spyLogger).error(msg);
    }
}