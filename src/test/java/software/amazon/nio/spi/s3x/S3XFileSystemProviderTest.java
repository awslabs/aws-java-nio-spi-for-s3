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
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.BDDAssertions.then;
import org.junit.jupiter.api.Test;
import software.amazon.nio.spi.s3.FakeAsyncS3ClientBuilder;
import software.amazon.nio.spi.s3.S3FileSystem;
import software.amazon.nio.spi.s3.S3Path;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_REGION_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_ACCESS_KEY_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_SECRET_ACCESS_KEY_PROPERTY;


/**
 *
 */
public class S3XFileSystemProviderTest {

    final static FakeAsyncS3ClientBuilder BUILDER = new FakeAsyncS3ClientBuilder();

    final static URI URI1 = URI.create("s3x://myendpoint/foo");
    final static URI URI2 = URI.create("s3x://myendpoint/foo/baa2");
    final static URI URI3 = URI.create("s3x://myendpoint.com:1010/foo/baa2/dir");
    final static URI URI4 = URI.create("s3x://myendpoint.com:1010/foo2/baa2");
    final static URI URI5 = URI.create("s3x://myendpoint.com:1010/foo2/dir2");
    final static URI URI6 = URI.create("s3x://myendpoint.com:1234/foo3/baa2");
    final static URI URI7 = URI.create("s3x://key:secret@myendpoint.com:1010/foo/baa2");
    final static URI URI8 = URI.create("s3x://key:anothersecret@myendpoint.com:1010/foo/baa2");
    final static URI URI9 = URI.create("s3x://akey:asecret@somewhere.com:2020/foo2/baa2");
    final static URI URI10 = URI.create("s3x://akey:anothersecret@somewhere.com:2020/foo2/baa2");

    @Test
    public void nio_provider() {
        S3Path path = (S3Path)Paths.get(URI.create("s3x://myendpoint/mybucket/myfolder"));

        S3FileSystem fs = path.getFileSystem();
        then(fs.provider()).isInstanceOf(S3XFileSystemProvider.class);
        then(fs.configuration().getEndpoint()).isEqualTo("myendpoint");
        then(fs.configuration().getBucketName()).isEqualTo("mybucket");
        then(path.getKey()).isEqualTo("myfolder");
    }

    @Test
    public void newFileSystem() {
        S3XFileSystemProvider provider = new S3XFileSystemProvider();

        //
        // a first file system
        //
        S3FileSystem fs = provider.newFileSystem(URI1);
        then(fs).isNotNull();
        then(fs.configuration().getBucketName()).isEqualTo("foo");
        then(fs.configuration().getCredentials()).isNull();

        //
        // the same file system can not be created twice
        //
        assertThatCode(() -> provider.newFileSystem(URI1))
                .as("filesystem created twice!")
                .isInstanceOf(FileSystemAlreadyExistsException.class)
                .hasMessageContaining("'myendpoint/foo'");

        //
        // Same bucket but different path, is still the same file system
        //
        assertThatCode(() -> provider.newFileSystem(URI2))
                .as("filesystem created twice!")
                .isInstanceOf(FileSystemAlreadyExistsException.class)
                .hasMessageContaining("'myendpoint/foo'");
        provider.closeFileSystem(fs);

        //
        // With endpoint no credentials
        //
        fs = provider.newFileSystem(URI3);
        then(fs.configuration().getCredentials()).isNull();
        then(fs.configuration().getBucketName()).isEqualTo("foo");
        then(fs.configuration().getEndpoint()).isEqualTo("myendpoint.com:1010");
        provider.closeFileSystem(fs);

        //
        // With existing endpoint, different bucket, no credentials
        //
        fs = provider.newFileSystem(URI4);
        then(fs.configuration().getCredentials()).isNull();
        then(fs.configuration().getBucketName()).isEqualTo("foo2");
        then(fs.configuration().getEndpoint()).isEqualTo("myendpoint.com:1010");

        //
        // With existing endpoint and bucket, no credentials

        //
        assertThatCode(() -> provider.newFileSystem(URI5))
                .as("filesystem created twice!")
                .isInstanceOf(FileSystemAlreadyExistsException.class)
                .hasMessageContaining("'myendpoint.com:1010/foo2'");
        provider.closeFileSystem(fs);

        //
        // Same hostname, different port, different bucket, no credentials
        //
        fs = provider.newFileSystem(URI6);
        then(fs.configuration().getCredentials()).isNull();
        then(fs.configuration().getBucketName()).isEqualTo("foo3");
        then(fs.configuration().getEndpoint()).isEqualTo("myendpoint.com:1234");
        provider.closeFileSystem(fs);

        //
        // With existing endpoint, same bucket, credentials
        //
        fs = provider.newFileSystem(URI9);
        then(fs.configuration().getCredentials().accessKeyId()).isEqualTo("akey");
        then(fs.configuration().getCredentials().secretAccessKey()).isEqualTo("asecret");
        then(fs.configuration().getBucketName()).isEqualTo("foo2");
        then(fs.configuration().getEndpoint()).isEqualTo("somewhere.com:2020");

        //
        // With same endpoint, same bucket, different credentials
        //
        assertThatCode(() -> provider.newFileSystem(URI10))
                .as("filesystem created twice!")
                .isInstanceOf(FileSystemAlreadyExistsException.class)
                .hasMessageContaining("'akey@somewhere.com:2020/foo2");

        provider.closeFileSystem(fs);
    }

    @Test
    public void getFileSystem() {
        S3XFileSystemProvider provider = new S3XFileSystemProvider();

        FileSystem fs2 = provider.newFileSystem(URI1);
        then(provider.getFileSystem(URI1)).isSameAs(fs2);
        FileSystem fs3 = provider.newFileSystem(URI3);
        then(fs3).isNotSameAs(fs2);
        then(provider.getFileSystem(URI2)).isSameAs(fs2);
        then(provider.newFileSystem(URI7)).isNotSameAs(fs3);
        then(provider.getFileSystem(URI8)).isNotSameAs(fs3);
        provider.closeFileSystem(fs2);
        provider.closeFileSystem(fs3);

        assertThatCode(() -> provider.getFileSystem(URI.create("s3://nowhere.com:2000/foo2/baa2")))
                .as("missing error")
                .isInstanceOf(FileSystemNotFoundException.class)
                .hasMessageContaining("file system not found for 'nowhere.com:2000/foo2'");
    }

    @Test
    public void setEndpointProtocolThroughConfiguration() throws Exception {
        S3NioSpiConfiguration env = new S3NioSpiConfiguration();

        S3XFileSystemProvider p = new S3XFileSystemProvider();

        S3FileSystem fs = p.newFileSystem(URI.create("s3://some.where.com:1010/bucket"), env);
        fs.clientProvider().asyncClientBuilder(BUILDER);
        fs.client(); fs.close();

        then(fs.bucketName()).isEqualTo("bucket");
        then(fs.configuration().getEndpoint()).isEqualTo("some.where.com:1010");
        then(BUILDER.endpointOverride.toString()).isEqualTo("https://some.where.com:1010");

        env.withEndpointProtocol("http");

        fs = p.newFileSystem(URI.create("s3://any.where.com:2020/foo"), env);
        fs.clientProvider().asyncClientBuilder(BUILDER);
        fs.client(); fs.close();

        then(fs.bucketName()).isEqualTo("foo");
        then(fs.configuration().getEndpoint()).isEqualTo("any.where.com:2020");
        then(BUILDER.endpointOverride.toString()).isEqualTo("http://any.where.com:2020");
    }
    
    @Test
    public void setForcePathStyleThroughConfiguration() throws Exception {
        S3NioSpiConfiguration env = new S3NioSpiConfiguration();

        S3XFileSystemProvider p = new S3XFileSystemProvider();

        //
        // true by default
        //
        S3FileSystem fs = p.newFileSystem(URI.create("s3x://some.where.com:1010/bucket"), env);
        fs.clientProvider().asyncClientBuilder(BUILDER);
        fs.client(); fs.close();

        then(BUILDER.forcePathStyle).isTrue();

        //
        // false by withForcePath()
        //
        env.withForcePathStyle(false);
        fs = p.newFileSystem(URI.create("s3x://any.where.com:2020/foo"), env);
        fs.clientProvider().asyncClientBuilder(BUILDER);
        fs.client(); fs.close();

        then(BUILDER.forcePathStyle).isFalse();
        
        //
        // false if set directly to false
        //
        env.put(S3NioSpiConfiguration.S3_SPI_FORCE_PATH_STYLE_PROPERTY, false);
        fs = p.newFileSystem(URI.create("s3x://any.where.com:2020/foo"), env);
        fs.clientProvider().asyncClientBuilder(BUILDER);
        fs.client(); fs.close();

        then(BUILDER.forcePathStyle).isFalse();
    }

    @Test
    public void setCredentialsThroughMap() throws Exception {
        S3XFileSystemProvider p = new S3XFileSystemProvider();
        Map<String, String> env = new HashMap<>();
        env.put(AWS_ACCESS_KEY_PROPERTY, "envkey");
        env.put(AWS_SECRET_ACCESS_KEY_PROPERTY, "envsecret");

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");
            System.setProperty(AWS_ACCESS_KEY_PROPERTY, "systemkey");
            System.setProperty(AWS_SECRET_ACCESS_KEY_PROPERTY, "systemsecret");

            S3FileSystem fs = p.newFileSystem(URI.create("s3x://some.where.com:1010/bucket"), env);
            fs.clientProvider().asyncClientBuilder(BUILDER);
            fs.client(); fs.close();

            then(fs.configuration().getBucketName()).isEqualTo("bucket");
            then(fs.configuration().getEndpoint()).isEqualTo("some.where.com:1010");
            then(BUILDER.endpointOverride.toString()).isEqualTo("https://some.where.com:1010");
            then(BUILDER.credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo("envkey");
            then(BUILDER.credentialsProvider.resolveCredentials().secretAccessKey()).isEqualTo("envsecret");
        });
    }

    @Test
    public void setCredentialsThroughURI() throws Exception {
        S3XFileSystemProvider p = new S3XFileSystemProvider();
        Map<String, String> env = new HashMap<>();
        env.put(AWS_ACCESS_KEY_PROPERTY, "envkey");
        env.put(AWS_SECRET_ACCESS_KEY_PROPERTY, "envsecret");

        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");

            S3FileSystem fs = p.newFileSystem(URI.create("s3://urikey:urisecret@some.where.com:1010/bucket"), env);
            fs.clientProvider().asyncClientBuilder(BUILDER);
            fs.client(); fs.close();

            then(fs.configuration().getBucketName()).isEqualTo("bucket");
            then(fs.configuration().getEndpoint()).isEqualTo("some.where.com:1010");
            then(BUILDER.endpointOverride.toString()).isEqualTo("https://some.where.com:1010");
            then(BUILDER.credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo("urikey");
            then(BUILDER.credentialsProvider.resolveCredentials().secretAccessKey()).isEqualTo("urisecret");
        });
    }

    @Test
    public void getPath() {
        S3XFileSystemProvider p = new S3XFileSystemProvider();

        then(p.getPath(URI1)).isNotNull();

        //
        // Make sure a file system is created if not already done (if the file
        // system is has not been created getFilaSystem would throw an exception)
        //
        p.closeFileSystem(p.getFileSystem(URI1));
    }

    @Test
    public void getS3FileSystemFromS3XURI() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put(AWS_REGION_PROPERTY, "us-west-1");

        S3FileSystem fs = (S3FileSystem)FileSystems.newFileSystem(URI7, Collections.EMPTY_MAP);
        then(fs).isNotNull();
        assertThatThrownBy(() -> FileSystems.newFileSystem(URI7, Collections.EMPTY_MAP))
                .isInstanceOf(FileSystemAlreadyExistsException.class)
                .hasMessage("a file system already exists for 'key@myendpoint.com:1010/foo', use getFileSystem() instead");

        then(FileSystems.getFileSystem(URI7)).isSameAs(fs);
        fs.close();
    }
}
