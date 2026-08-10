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
import org.junit.jupiter.api.DisplayName;
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
        s3FileSystem = (S3FileSystem) provider.getPath(s3Uri).getFileSystem();
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
        s3FileSystem.clientProvider(P1);
        then(s3FileSystem.clientProvider()).isSameAs(P1);
        s3FileSystem.clientProvider(P2);
        then(s3FileSystem.clientProvider()).isSameAs(P2);
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
        var stores = s3FileSystem.getFileStores().iterator();
        assertTrue(stores.hasNext());
        var store = stores.next();
        assertEquals(s3FileSystem.bucketName(), store.name());
        assertEquals("s3", store.type());
        assertFalse(stores.hasNext());
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

        // Temp files are created atomically with a unique name (a random suffix guarantees
        // uniqueness), so they live directly under the temporary directory for top-level keys.
        var tempFile1 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, "file1"));
        then(tempFile1).exists().isRegularFile();
        then(tempFile1.getParent()).isEqualTo(temporaryDirectory);

        var tempFile2 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, "/file2"));
        then(tempFile2).exists().isRegularFile();
        then(tempFile2.getParent()).isEqualTo(temporaryDirectory);

        // For nested keys the parent directory structure is mirrored beneath the temp directory.
        var tempFile3 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, "/dir1/dir2/file3"));
        then(tempFile3).exists().isRegularFile();
        then(tempFile3.getParent()).isEqualTo(temporaryDirectory.resolve("dir1/dir2"));

        var tempFile4 = s3FileSystem.createTempFile(S3Path.getPath(s3FileSystem, "dir1/dir2/file4"));
        then(tempFile4).exists().isRegularFile();
        then(tempFile4.getParent()).isEqualTo(temporaryDirectory.resolve("dir1/dir2"));
    }

    @DisplayName("An S3 object can be opened with multiple channels, so we need to enable multiple temporary files.")
    @Test
    void createTempFile_alreadyExists() throws IOException {
        var temporaryDirectory = s3FileSystemTemporaryDirectory();

        var key = "somefile";
        var path = S3Path.getPath(s3FileSystem, key);

        var first = s3FileSystem.createTempFile(path);
        then(first).exists().isRegularFile();
        then(first.getParent()).isEqualTo(temporaryDirectory);

        // A second channel for the same key must get a distinct temp file, without any collision.
        var second = s3FileSystem.createTempFile(path);
        then(second)
            .exists()
            .isRegularFile()
            .isNotEqualTo(first);
        then(second.getParent()).isEqualTo(temporaryDirectory);
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
