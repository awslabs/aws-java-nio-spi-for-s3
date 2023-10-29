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

package software.amazon.nio.spi.s3x;

import java.net.URI;
import software.amazon.nio.spi.s3.S3FileSystemProvider;
import software.amazon.nio.spi.s3.util.S3FileSystemInfo;
import software.amazon.nio.spi.s3x.util.S3XFileSystemInfo;

/**
 *
 */
public class S3XFileSystemProvider extends S3FileSystemProvider {

    public static final String SCHEME = "s3x";

    /**
     * Returns the URI scheme that identifies this provider.
     *
     * @return The URI scheme (s3x)
     */
    @Override
    public String getScheme() {
        return SCHEME;
    }

    /**
     * This overrides the default AWS implementation to be able to address 3rd
     * party S3 services. To do so, we relax the default S3 URI format to the
     * following:
     *
     * {@code
     *
     * s3x://[accessKey:accessSecret@]endpoint/bucket/key
     *
     * }
     *
     * Please note that the authority part of the URI (endpoint above) is always
     * considered a HTTP(S) endpoint, therefore the name of the bucket is the
     * first element of the path. The remaining path elements will be the object
     * key.
     *
     * @param uri the URI to extract the information from
     *
     * @return the information extracted from {@code uri}
     */
    @Override
    protected S3FileSystemInfo fileSystemInfo(URI uri) {
        return new S3XFileSystemInfo(uri);
    }

}
