/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.mockito.ArgumentMatchers;

import java.util.function.Consumer;

public class S3Matchers {
    public static <T> Consumer<T> anyConsumer() {
        return ArgumentMatchers.any();
    }
}
