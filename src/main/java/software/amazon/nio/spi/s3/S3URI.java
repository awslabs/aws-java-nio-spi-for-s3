/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.nio.spi.s3;

import java.net.URI;
import java.util.Objects;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.nio.spi.s3.util.StringUtils;

/**
 * A record that splits a URI into its components.
 *
 * @param bucket the bucket name this URI represents - NOT NULL
 * @param endpoint the endpoint of the service providing this bucket - CAN BE NULL
 *                 which means AWS
 * @param accessKey the provided accesskey to access the bucket - CAN BE NULL
 * @param secretAccessKey the provided secretAccesskey to access the bucket - NOT NULL if accessKey is not null
 * @param path the the resource to that this URI represents within the bucket - CAN BE NULL
 */
public record S3URI(String bucket, String endpoint, String accessKey, String secretAccessKey, String path) {

    public S3URI(String bucket) {
        this(bucket, null, null, null, null);
    }

    public S3URI(String bucket, String endpoint, String accessKey, String secretAccessKey, String path) {
        Objects.requireNonNull(bucket, "bucket can not be null");
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        if (accessKey != null) {
            Objects.requireNonNull(secretAccessKey, "secretAccessKey can not be null if accessKey is not null");
            this.secretAccessKey = secretAccessKey;
        } else {
            this.secretAccessKey = null;
        }
        this.path = path;
    }

    public AwsCredentials credentials() {
        return (accessKey() == null) ? null : AwsBasicCredentials.create(accessKey(), secretAccessKey());
    }

    public String fileSystemKey() {
        return ((endpoint() != null) ? (endpoint() + '/') : "") + bucket();
    }

    public static S3URI of(URI uri) {
        Objects.requireNonNull(uri, "uri can not be null");

        if (uri.getScheme() == null) {
            throw new IllegalArgumentException(
                String.format("invalid uri '%s', please provide an uri as s3://[key:secret@][host:port]/bucket", uri.toString())
            );
        }
        if (uri.getAuthority() == null) {
            throw new IllegalArgumentException(
                String.format("invalid uri '%s', please provide an uri as s3://[key:secret@][host:port]/bucket", uri.toString())
            );
        }

        String bucket, endpoint, path;
        String host = uri.getHost(); int port = uri.getPort();
        String[] elements = uri.getPath().split(S3Path.PATH_SEPARATOR);
        if ((port > 0) || (host.indexOf(':') > 0)) {
            endpoint = host + ((port > 0) ? (":" + port) : "");
            bucket = elements[1];
            path = StringUtils.join(S3Path.PATH_SEPARATOR, elements, 2);
        } else {
            bucket = host;
            endpoint = null;
            path = StringUtils.join(S3Path.PATH_SEPARATOR, elements, 1);
        }

        String key = null, secret = null;
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            int pos = userInfo.indexOf(':');
            key = (pos < 0) ? userInfo : userInfo.substring(0, pos);
            secret = (pos < 0) ? null : userInfo.substring(pos+1);
        }

        return new S3URI(bucket, endpoint, key, secret, path);
    }
}