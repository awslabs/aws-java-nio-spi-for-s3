/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.BDDAssertions.*;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3AssumeObjectNotExistsTest {

    @Test
    void test_apply() {
        var putObjectRequest = PutObjectRequest.builder();
        then(putObjectRequest.build().ifNoneMatch()).isNull();
        S3AssumeObjectNotExists.INSTANCE.apply(putObjectRequest, null);
        then(putObjectRequest.build().ifNoneMatch()).isEqualTo("*");
    }

    @Test
    void test_copy() {
        var instance = S3AssumeObjectNotExists.INSTANCE;
        then(instance.copy())
            .isSameAs(instance)
            .isSameAs(S3OpenOption.assumeObjectNotExists())
            .isSameAs(S3AssumeObjectNotExists.INSTANCE);
    }
}
