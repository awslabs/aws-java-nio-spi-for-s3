/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static com.github.stefanbirkner.systemlambda.SystemLambda.restoreSystemProperties;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
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
        then(fs.configuration().getEndpoint()).isEqualTo("myendpoint");
        then(fs.configuration().getBucketName()).isEqualTo("mybucket");
        then(path.getKey()).isEqualTo("myfolder");
    }

    @Test
    @DisplayName("newFileSystem(URI, env) should throw")
    public void newFileSystemURI() {
        assertThatThrownBy(
            () -> new S3XFileSystemProvider().newFileSystem(URI1, Collections.emptyMap())
        ).isInstanceOf(NotYetImplementedException.class);
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

        FileSystem fs2 = provider.getFileSystem(URI1, true);
        then(provider.getFileSystem(URI1)).isSameAs(fs2);
        FileSystem fs3 = provider.getFileSystem(URI3, true);
        then(fs3).isNotSameAs(fs2);
        then(provider.getFileSystem(URI2)).isSameAs(fs2);
        then(provider.getFileSystem(URI7, true)).isNotSameAs(fs3);
        then(provider.getFileSystem(URI8)).isNotSameAs(fs3);
        provider.closeFileSystem(fs2);
        provider.closeFileSystem(fs3);

        assertThatCode(() -> provider.getFileSystem(URI.create("s3://nowhere.com:2000/foo2/baa2")))
                .as("missing error")
                .isInstanceOf(FileSystemNotFoundException.class)
                .hasMessageContaining("file system not found for 'nowhere.com:2000/foo2'");
    }

    @Test
    public void setCredentialsThroughURI() throws Exception {
        var p = new S3XFileSystemProvider();
        var BUILDER = spy(S3AsyncClient.crtBuilder());
        restoreSystemProperties(() -> {
            System.setProperty("aws.region", "us-west-1");

            var fs = p.getFileSystem(URI.create("s3x://urikey:urisecret@some.where.com:1010/bucket"), true);
            fs.clientProvider().asyncClientBuilder(BUILDER);
            fs.client();
            fs.close();

            then(fs.configuration().getBucketName()).isEqualTo("bucket");
            then(fs.configuration().getEndpoint()).isEqualTo("some.where.com:1010");

            verify(BUILDER).endpointOverride(URI.create("https://some.where.com:1010"));
            then(fs.configuration().getCredentials().accessKeyId()).isEqualTo("urikey");
            then(fs.configuration().getCredentials().secretAccessKey()).isEqualTo("urisecret");

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

}
