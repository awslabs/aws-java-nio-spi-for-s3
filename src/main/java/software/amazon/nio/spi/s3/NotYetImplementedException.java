/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

public class NotYetImplementedException extends RuntimeException {
    public NotYetImplementedException(String message) {
        super(message);
    }
}
