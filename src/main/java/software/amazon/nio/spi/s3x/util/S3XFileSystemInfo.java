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

package software.amazon.nio.spi.s3x.util;

import java.net.URI;
import software.amazon.awssdk.services.s3.internal.BucketUtils;
import software.amazon.nio.spi.s3.S3Path;
import software.amazon.nio.spi.s3.util.S3FileSystemInfo;

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
public class S3XFileSystemInfo extends S3FileSystemInfo {

    /**
     * Creates a new instance and populates it with the information extracted
     * from {@code uri}. The provided uri is parsed accordingly to the following
     * format:
     *
     * {@code
     *
     * s3x://[accessKey:accessSecret@]endpoint[:port]/bucket/key
     *
     * }
     *
     * Please note that the authority part of the URI (endpoint[:port] above) is always
     * considered a HTTP(S) endpoint, therefore the name of the bucket is the
     * first element of the path. The remaining path elements will be the object
     * key.
     *
     * Additionally {@code key} is computed as endpoint/bucket/accessKey
     *
     * @param uri a S3 URI
     *
     * @throws IllegalArgumentException if URI contains invalid components
     *         (e.g. an invalid bucket name)
     */
    public S3XFileSystemInfo(URI uri) throws IllegalArgumentException {
        if (uri == null) {
            throw new IllegalArgumentException("uri can not be null");
        }

        final String userInfo = uri.getUserInfo();

        if (userInfo != null) {
            int pos = userInfo.indexOf(':');
            accessKey = (pos < 0) ? userInfo : userInfo.substring(0, pos);
            accessSecret = (pos < 0) ? null : userInfo.substring(pos+1);
        } else {
            accessKey = accessSecret = null;
        }

        endpoint = uri.getHost();
        if (uri.getPort() > 0) {
            endpoint += ":" + uri.getPort();
        }
        bucket = uri.getPath().split(S3Path.PATH_SEPARATOR)[1];

        BucketUtils.isValidDnsBucketName(bucket, true);

        key = endpoint + '/' + bucket;
        if (accessKey != null) {
            key = accessKey + '@' + key;
        }
    }
}
