/*
 * Copyright 2023 ste.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.nio.spi.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 *
 */
public class FakeS3ClientProvider extends S3ClientProvider {

    final public AwsClient client;

    public FakeS3ClientProvider(S3AsyncClient client) {
        this.client = client;
    }

    @Override
    public S3Client universalClient() {
        return (S3Client)client;
    }

    @Override
    protected S3AsyncClient generateAsyncClient(String endpoint, String bucketName, AwsCredentials credentials) {
        return (S3AsyncClient)client;
    }

    @Override
    protected S3Client generateClient (String endpoint, String bucket, AwsCredentials credentials) {
        return (S3Client)client;
    }

}