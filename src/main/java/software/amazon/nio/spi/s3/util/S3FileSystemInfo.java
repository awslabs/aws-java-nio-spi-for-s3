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

package software.amazon.nio.spi.s3.util;

import java.net.URI;
import software.amazon.awssdk.services.s3.internal.BucketUtils;

/**
 * Populates fields with information extracted by the S3 URI provided. This
 * implementation is for standard AWS buckets as described in section
 * "Accessing a bucket using S3://" in https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html
 *
 * It also computes the file system key that can be used to identify a runtime
 * instance of a S3FileSystem (for caching purposes for example). In this
 * implementation the key is the bucket name (which is unique in the AWS S3
 * namespace).
 *
 */
public class S3FileSystemInfo {

    protected String key;
    protected String endpoint;
    protected String bucket;
    protected String accessKey;
    protected String accessSecret;

    protected S3FileSystemInfo() {}

    /**
     * Creates a new instance and populates it with key and bucket. The name of
     * the bucket must follow AWS S3 bucket naming rules (https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)
     *
     * @param uri a S3 URI
     *
     * @throws IllegalArgumentException if URI contains invalid components
     *         (e.g. an invalid bucket name)
     */
    public S3FileSystemInfo(URI uri) throws IllegalArgumentException {
        if (uri == null) {
            throw new IllegalArgumentException("uri can not be null");
        }

        key = bucket = uri.getAuthority();
        endpoint = accessKey = accessSecret = null;

        BucketUtils.isValidDnsBucketName(bucket, true);
    }

    public String key() { return key; }
    public String endpoint() { return endpoint; }
    public String bucket() { return bucket; }
    public String accessKey() { return accessKey; }
    public String accessSecret() { return accessSecret; }
}
