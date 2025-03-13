/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.nio.spi.s3.Constants.PATH_SEPARATOR;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@ExtendWith(MockitoExtension.class)
public class S3FileSystemTest {
    S3FileSystemProvider provider;
    URI s3Uri = URI.create("s3://mybucket/some/path/to/object.txt");
    S3FileSystem s3FileSystem;

    @Mock
    S3AsyncClient mockClient; //client used to determine bucket location

    @BeforeEach
    public void init() {
        provider = new S3FileSystemProvider();
        s3FileSystem = (S3FileSystem) provider.getFileSystem(s3Uri);
        s3FileSystem.clientProvider = new FixedS3ClientProvider(mockClient);
    }

    @AfterEach
    public void after() throws Exception {
        s3FileSystem.close();
    }

    @Test
    public void getSeparator() {
        assertEquals("/", provider.getFileSystem(s3Uri).getSeparator());
    }

    @Test
    public void close() throws IOException {
        assertEquals(0, s3FileSystem.getOpenChannels().size());
        s3FileSystem.close();
        assertFalse(s3FileSystem.isOpen(), "File system should return false from isOpen when closed has been called");

        // close() should also remove the instance from the provider
        assertFalse(provider.getFsCache().containsKey(s3Uri.toString()));
    }

    @Test
    public void isOpen() {
        assertTrue(s3FileSystem.isOpen(), "File system should be open when newly created");
    }

    @Test
    public void bucketName() {
        assertEquals("mybucket", s3FileSystem.bucketName());
    }

    @Test
    public void isReadOnly() {
        assertFalse(s3FileSystem.isReadOnly());
    }

    @Test
    public void getAndSetClientProvider() {
        final var P1 = new S3ClientProvider(null);
        final var P2 = new S3ClientProvider(null);
        s3FileSystem.clientProvider(P1); then(s3FileSystem.clientProvider()).isSameAs(P1);
        s3FileSystem.clientProvider(P2); then(s3FileSystem.clientProvider()).isSameAs(P2);
    }

    @Test
    public void getRootDirectories() {
        final var rootDirectories = s3FileSystem.getRootDirectories();
        assertNotNull(rootDirectories);

        final var rootDirectoriesIterator = rootDirectories.iterator();

        assertTrue(rootDirectoriesIterator.hasNext());
        assertEquals(PATH_SEPARATOR, rootDirectoriesIterator.next().toString());
        assertFalse(rootDirectoriesIterator.hasNext());
    }

    @Test
    public void getFileStores() {
        assertEquals(Collections.EMPTY_SET, s3FileSystem.getFileStores());
    }

    @Test
    public void supportedFileAttributeViews() {
        assertTrue(s3FileSystem.supportedFileAttributeViews().contains("basic"));
    }

    @Test
    public void getPath() {
        //additional path construction tests are in S3PathTest
        assertEquals(s3FileSystem.getPath("/"), S3Path.getPath(s3FileSystem, PATH_SEPARATOR));
    }

    @Test
    public void getPathMatcher() {
        assertEquals(FileSystems.getDefault().getPathMatcher("glob:*.*").getClass(),
                s3FileSystem.getPathMatcher("glob:*.*").getClass());
    }

    @Test
    void createTempFile() throws IOException {
        var temporaryDirectory = s3FileSystemTemporaryDirectory();

        thenThrownBy(() -> s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, "/dir/")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("path must be a file");
        thenThrownBy(() -> s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, "/dir1/dir2/dir3/")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("path must be a file");

        var key1 = "file1";
        var tempFile1 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, key1));
        then(tempFile1).exists().isEqualTo(temporaryDirectory.resolve(key1));

        var key2 = "/file2";
        var tempFile2 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, key2));
        then(tempFile2).exists().isEqualTo(temporaryDirectory.resolve(key2.substring(1)));

        var key3 = "/dir1/dir2/file3";
        var tempFile3 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, key3));
        then(tempFile3).exists().isEqualTo(temporaryDirectory.resolve(key3.substring(1)));

        var key4 = "dir1/dir2/file4";
        var tempFile4 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, key4));
        then(tempFile4).exists().isEqualTo(temporaryDirectory.resolve(key4));
    }

    private Path s3FileSystemTemporaryDirectory() throws IOException {
        var tempFile0 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, "file0"));
        var temporaryDirectory = tempFile0.getParent();
        Files.delete(tempFile0);
        return temporaryDirectory;
    }

    @Test
    public void testGetOpenChannelsIsNotModifiable() {
        //
        // thrown because cannot be modified
        //
        assertThrows(UnsupportedOperationException.class, () -> s3FileSystem.getOpenChannels().add(null));
    }
}
