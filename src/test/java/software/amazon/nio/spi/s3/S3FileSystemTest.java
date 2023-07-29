/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import static org.assertj.core.api.BDDAssertions.then;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.lenient;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

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
        s3FileSystem = this.provider.newFileSystem(s3Uri, Collections.emptyMap());
        s3FileSystem.clientProvider = new FixedS3ClientProvider(mockClient);
        lenient().when(mockClient.headObject(any(Consumer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
    }

    @AfterEach
    public void after() throws Exception {
        s3FileSystem.close();
    }

    @Test
    public void getSeparator() {
        assertEquals("/", new S3FileSystem(s3Uri, provider).getSeparator());
    }


    @Test
    public void close() throws IOException {
        assertEquals(0, s3FileSystem.getOpenChannels().size());
        s3FileSystem.close();
        assertFalse(s3FileSystem.isOpen(), "File system should return false from isOpen when closed has been called");
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
        final S3ClientProvider P1 = new S3ClientProvider();
        final S3ClientProvider P2 = new S3ClientProvider();
        s3FileSystem.clientProvider(P1); then(s3FileSystem.clientProvider()).isSameAs(P1);
        s3FileSystem.clientProvider(P2); then(s3FileSystem.clientProvider()).isSameAs(P2);
    }

    @Test
    public void getRootDirectories() {
        final Iterable<Path> rootDirectories = s3FileSystem.getRootDirectories();
        assertNotNull(rootDirectories);

        final Iterator<Path> rootDirectoriesIterator = rootDirectories.iterator();

        assertTrue(rootDirectoriesIterator.hasNext());
        assertEquals(S3Path.PATH_SEPARATOR, rootDirectoriesIterator.next().toString());
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
        assertEquals(s3FileSystem.getPath("/"), S3Path.getPath(s3FileSystem, S3Path.PATH_SEPARATOR));
    }

    @Test
    public void getPathMatcher() {
        assertEquals(FileSystems.getDefault().getPathMatcher("glob:*.*").getClass(),
                s3FileSystem.getPathMatcher("glob:*.*").getClass());
    }

    @Test
    public void testGetOpenChannelsIsNotModifiable() {
        //
        // thrown because cannot be modified
        //
        assertThrows(UnsupportedOperationException.class, () -> s3FileSystem.getOpenChannels().add(null));
    }
}
