/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import static software.amazon.awssdk.core.client.config.SdkClientOption.ENDPOINT;
import static software.amazon.awssdk.core.client.config.SdkClientOption.ENDPOINT_OVERRIDDEN;
import static software.amazon.awssdk.awscore.client.config.AwsClientOption.CREDENTIALS_PROVIDER;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;

@RunWith(MockitoJUnitRunner.class)
public class S3FileSystemTest {
    S3FileSystemProvider provider;
    URI s3Uri = URI.create("s3://mybucket/some/path/to/object.txt");
    S3FileSystem s3FileSystem;

    @Mock
    S3Client mockClient; //client used to determine bucket location

    @Before
    public void init() {
        this.provider = new S3FileSystemProvider();
        s3FileSystem = (S3FileSystem) this.provider.newFileSystem(s3Uri, Collections.emptyMap());
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
        assertEquals("mybucket", new S3FileSystem("s3://key:secret@endpoint/mybucket/myresource", provider).bucketName());
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

    @Test
    public void clientsWithProvidedEndpoint() throws Exception {
        when(mockClient.getBucketLocation(any(Consumer.class)))
                .thenReturn(GetBucketLocationResponse.builder().locationConstraint("us-west-2").build());
        S3FileSystemProvider.getClientStore().locationClient(mockClient);

        final String URI1 = "s3://endpoint1.io/bucket/resource";
        final String URI2 = "s3://endpoint2.io:8080/bucket/resource";

        S3FileSystem fs = new S3FileSystem(URI.create(URI1), provider);
        S3Client client = fs.client();

        Field f = client.getClass().getDeclaredField("clientConfiguration");
        f.setAccessible(true);
        SdkClientConfiguration sdkConf = (SdkClientConfiguration)f.get(client);

        assertTrue(sdkConf.option(ENDPOINT_OVERRIDDEN));
        assertEquals(URI.create("https://endpoint1.io"), sdkConf.option(ENDPOINT));

        sdkConf = (SdkClientConfiguration)f.get(new S3FileSystem(URI.create(URI2), provider).client());
        assertTrue(sdkConf.option(ENDPOINT_OVERRIDDEN));
        assertEquals(URI.create("https://endpoint2.io:8080"), sdkConf.option(ENDPOINT));
    }

    @Test
    public void clientsWithProvidedEndpointAndCredentials() throws Exception {
        when(mockClient.getBucketLocation(any(Consumer.class)))
                .thenReturn(GetBucketLocationResponse.builder().locationConstraint("us-west-2").build());
        S3FileSystemProvider.getClientStore().locationClient(mockClient);

        final String URI1 = "s3://key1:secret1@endpoint1.io/bucket/resource";
        final String URI2 = "s3://key2:secret2@endpoint2.io:8080/bucket/resource";

        S3FileSystem fs = new S3FileSystem(URI.create(URI1), provider);
        S3Client client = fs.client();

        Field f = client.getClass().getDeclaredField("clientConfiguration");
        f.setAccessible(true);
        SdkClientConfiguration sdkConf = (SdkClientConfiguration)f.get(client);

        assertEquals(URI.create("https://endpoint1.io"), sdkConf.option(ENDPOINT));
        assertFalse(sdkConf.option(CREDENTIALS_PROVIDER) instanceof DefaultCredentialsProvider);
        AwsCredentialsProvider credentials = (AwsCredentialsProvider)sdkConf.option(CREDENTIALS_PROVIDER);
        assertEquals("key1", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret1", credentials.resolveCredentials().secretAccessKey());


        sdkConf = (SdkClientConfiguration)f.get(new S3FileSystem(URI.create(URI2), provider).client());
        assertEquals(URI.create("https://endpoint2.io:8080"), sdkConf.option(ENDPOINT));
        credentials = (AwsCredentialsProvider)sdkConf.option(CREDENTIALS_PROVIDER);
        assertEquals("key2", credentials.resolveCredentials().accessKeyId());
        assertEquals("secret2", credentials.resolveCredentials().secretAccessKey());
    }

}