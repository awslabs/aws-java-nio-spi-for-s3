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
import software.amazon.nio.spi.s3.S3Path;

/**
 *
 */
public class S3FileSystemInfo {

    public final String key;
    public final String endpoint;
    public final String bucket;
    public final String accessKey;
    public final String accessSecret;

    public S3FileSystemInfo(URI uri) {
        final String host = uri.getHost();
        final String userInfo = uri.getUserInfo();
        final int port = uri.getPort();

        key = (port<0) && (!host.contains("."))
               ? host
               : (host + ((port < 0) ? "" : (":" + port)) + S3Path.PATH_SEPARATOR + uri.getPath().split(S3Path.PATH_SEPARATOR)[1]);

        if ((port > 0) || (host.indexOf(':') > 0)) {
            bucket = uri.getPath().split(S3Path.PATH_SEPARATOR)[1];
            endpoint = host + ((port > 0) ? (":" + port) : "");
        } else {
            bucket = host;
            endpoint = null;
        }

        if (userInfo != null) {
            int pos = userInfo.indexOf(':');
            accessKey = (pos < 0) ? userInfo : userInfo.substring(0, pos);
            accessSecret = (pos < 0) ? null : userInfo.substring(pos+1);
        } else {
            accessKey = accessSecret = null;
        }
    }
}
