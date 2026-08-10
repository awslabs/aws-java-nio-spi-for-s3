/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Paths;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3XFileSystemProviderTest {


    final static URI URI1 = URI.create("s3x://myendpoint/foo");
    final static URI URI2 = URI.create("s3x://myendpoint/foo/baa2");
    final static URI URI3 = URI.create("s3x://myendpoint.com:1010/foo/baa2/dir");
    final static URI URI7 = URI.create("s3x://key:secret@myendpoint.com:1010/foo/baa2");
    final static URI URI8 = URI.create("s3x://key:anothersecret@myendpoint.com:1010/foo/baa2");


    @Test
    public void nio_provider() {
        var path = (S3Path)Paths.get(URI.create("s3x://myendpoint/mybucket/myfolder"));

        var fs = path.getFileSystem();
        then(fs.provider()).isInstanceOf(S3XFileSystemProvider.class);
        then(fs.getConfiguration().getEndpoint()).isEqualTo("myendpoint");
        then(fs.getConfiguration().getBucketName()).isEqualTo("mybucket");
        then(path.getKey()).isEqualTo("myfolder");
    }

    @Test
    @DisplayName("newFileSystem(Path, env) should throw")
    public void newFileSystemPath() {
        assertThatThrownBy(
            () -> new S3XFileSystemProvider().newFileSystem(Paths.get(URI1), Collections.emptyMap())
        ).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void getFileSystem() {
        var provider = new S3XFileSystemProvider();

        // getFileSystem no longer lazily creates; a view is materialized via getPath and then
        // returned by getFileSystem for any URI resolving to the same file-system key.
        FileSystem fs2 = provider.getPath(URI1).getFileSystem();
        then(provider.getFileSystem(URI1)).isSameAs(fs2);
        FileSystem fs3 = provider.getPath(URI3).getFileSystem();
        then(fs3).isNotSameAs(fs2);
        then(provider.getFileSystem(URI2)).isSameAs(fs2);
        then(provider.getPath(URI7).getFileSystem()).isNotSameAs(fs3);
        then(provider.getPath(URI8).getFileSystem()).isNotSameAs(fs3);
        provider.closeFileSystem(fs2);
        provider.closeFileSystem(fs3);
    }

    @Test
    public void setCredentialsThroughURI() throws Exception {
        var p = new S3XFileSystemProvider();
        var BUILDER = spy(S3AsyncClient.crtBuilder());
        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");

            var fs = (S3FileSystem) p.getPath(URI.create("s3x://urikey:urisecret@some.where.com:1010/bucket")).getFileSystem();
            fs.clientProvider().asyncClientBuilder(BUILDER);
            fs.client();
            fs.close();

            then(fs.getConfiguration().getBucketName()).isEqualTo("bucket");
            then(fs.getConfiguration().getEndpoint()).isEqualTo("some.where.com:1010");

            verify(BUILDER).endpointOverride(URI.create("https://some.where.com:1010"));
            then(fs.getConfiguration().getCredentials().accessKeyId()).isEqualTo("urikey");
            then(fs.getConfiguration().getCredentials().secretAccessKey()).isEqualTo("urisecret");

        });
    }

    @Test
    public void getPath() {
        var p = new S3XFileSystemProvider();
        then(p.getPath(URI1)).isNotNull();

        // Make sure a file system is created if not already done (if the file
        // system has not been created getFileSystem would throw an exception)
        p.closeFileSystem(p.getFileSystem(URI1));
    }

    @Test
    @DisplayName("env-map credentials override credentials parsed from the s3x URI")
    public void envMapCredentialsOverrideUriCredentials() throws Exception {
        // Reproduce the real config-merge path without the remote createBucket call.
        var provider = new S3XFileSystemProvider() {
            @Override
            public FileSystem newFileSystem(final URI uri, final java.util.Map<String, ?> env) {
                var info = fileSystemInfo(uri);
                var config = new software.amazon.nio.spi.s3.config.S3NioSpiConfiguration()
                    .withEndpoint(info.endpoint()).withBucketName(info.bucket());
                if (info.accessKey() != null) {
                    config.withCredentials(info.accessKey(), info.accessSecret());
                }
                config.withOverrides(env);
                return getOrCreateFileSystem(info.key(), config);
            }
        };

        var envCreds = software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("envKey", "envSecret");
        var fs = (S3FileSystem) provider.newFileSystem(URI7,
            java.util.Map.of(software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.S3_SPI_CREDENTIALS_PROPERTY, envCreds));
        try {
            // URI7 carries key:secret, but the env-map credentials win.
            then(fs.getConfiguration().getCredentials()).isSameAs(envCreds);
        } finally {
            provider.closeFileSystem(fs);
        }
    }

}
