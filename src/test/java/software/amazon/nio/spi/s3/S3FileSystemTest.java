/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.After;

import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.lenient;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@RunWith(MockitoJUnitRunner.class)
public class S3FileSystemTest {
    S3FileSystemProvider provider;
    URI s3Uri = URI.create("s3://mybucket/some/path/to/object.txt");
    S3FileSystem s3FileSystem;

    @Mock
    S3AsyncClient mockClient;

    @Before
    public void init() {
        provider = new S3FileSystemProvider();
        provider.clientProvider = new S3ClientProvider() {
            @Override
            protected S3AsyncClient generateAsyncClient(String bucketName) {
                return mockClient;
            }
        };
        s3FileSystem = (S3FileSystem) this.provider.newFileSystem(s3Uri, Collections.emptyMap());
        lenient().when(mockClient.headObject(any(Consumer.class))).thenReturn(
                CompletableFuture.supplyAsync(() -> HeadObjectResponse.builder().contentLength(100L).build()));
    }

    @After
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
        assertFalse("File system should return false from isOpen when closed has been called", s3FileSystem.isOpen());
    }

    @Test
    public void isOpen() {
        assertTrue("File system should be open when newly created", s3FileSystem.isOpen());
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
    public void getRootDirectories() {
        final Iterable<Path> rootDirectories = s3FileSystem.getRootDirectories();
        assertNotNull(rootDirectories);
        assertEquals(S3Path.PATH_SEPARATOR, rootDirectories.toString());
        assertFalse(s3FileSystem.getRootDirectories().iterator().hasNext());
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


    @Test(expected = UnsupportedOperationException.class)
    //thrown because cannot be modified
    public void testGetOpenChannelsIsNotModifiable() {
        s3FileSystem.getOpenChannels().add(null);
    }
}