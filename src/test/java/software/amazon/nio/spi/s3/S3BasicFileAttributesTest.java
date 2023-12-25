/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;
import software.amazon.nio.spi.s3.util.TimeOutUtils;

import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

@DisplayName("S3BasicFileAttributes")
public class S3BasicFileAttributesTest {

    private static final FileTime EPOCH_FILE_TIME = FileTime.from(Instant.EPOCH);

    @Nested
    @DisplayName("directory")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Directories {

        private S3BasicFileAttributes directoryAttributes;

        @BeforeAll
        void configureDirectory() throws IOException {
            S3FileSystem fs = mock();
            FileSystemProvider provider = mock();
            when(fs.provider()).thenReturn(provider);
            when(fs.configuration()).thenReturn(new S3NioSpiConfiguration());
            when(provider.getScheme()).thenReturn("s3");

            var directory = S3Path.getPath(fs, "/somedirectory/");
            directoryAttributes = S3BasicFileAttributes.get(directory, Duration.ofMinutes(TimeOutUtils.TIMEOUT_TIME_LENGTH_1));
        }

        @Test
        @DisplayName("lastModifiedTime() should return epoch")
        void lastModifiedTime() {
            assertThat(directoryAttributes.lastModifiedTime()).isEqualTo(EPOCH_FILE_TIME);
        }

        @Test
        @DisplayName("lastAccessTime() should return epoch")
        void lastAccessTime() {
            assertThat(directoryAttributes.lastAccessTime()).isEqualTo(EPOCH_FILE_TIME);
        }

        @Test
        @DisplayName("creationTime() should return epoch")
        void creationTime() {
            assertThat(directoryAttributes.creationTime()).isEqualTo(EPOCH_FILE_TIME);
        }

        @Test
        @DisplayName("size() should return 0")
        void size() {
            assertThat(directoryAttributes.size()).isZero();
        }

        @Test
        @DisplayName("fileKey() should return null")
        void fileKey() {
            assertThat(directoryAttributes.fileKey()).isNull();
        }

        @Test
        @DisplayName("isDirectory() should return true")
        void isDirectory() {
            assertThat(directoryAttributes.isDirectory()).isTrue();
        }

        @Test
        @DisplayName("isRegularFile() should return false")
        void isRegularFile() {
            assertThat(directoryAttributes.isRegularFile()).isFalse();
        }

        @Test
        @DisplayName("isSymbolicLink() should return false")
        void isSymbolicLink() {
            assertThat(directoryAttributes.isSymbolicLink()).isFalse();
        }

        @Test
        @DisplayName("isOther() should return false")
        void isOther() {
            assertThat(directoryAttributes.isOther()).isFalse();
        }
    }

    @Nested
    @DisplayName("regular file")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class RegularFiles {

        private S3BasicFileAttributes attributes;
        private final S3AsyncClient mockClient = mock();

        @BeforeAll
        void configureRegularFile() throws IOException {
            S3FileSystem fs = mock();

            FileSystemProvider provider = mock();
            when(fs.provider()).thenReturn(provider);
            when(provider.getScheme()).thenReturn("s3");

            when(fs.configuration()).thenReturn(new S3NioSpiConfiguration());

            when(fs.client()).thenReturn(mockClient);

            when(mockClient.headObject(anyConsumer())).thenReturn(
                CompletableFuture.supplyAsync(() ->
                    HeadObjectResponse.builder()
                        .lastModified(Instant.parse("2023-11-07T08:29:12.847553Z"))
                        .contentLength(100L)
                        .eTag("someEtag")
                        .build()
                )
            );

            var file = S3Path.getPath(fs, "somefile");
            attributes = S3BasicFileAttributes.get(file, Duration.ofMinutes(TimeOutUtils.TIMEOUT_TIME_LENGTH_1));
        }

        @BeforeEach
        void resetMockClient() {
            reset(mockClient);
        }

        @ParameterizedTest(name = "{0} should return the Instant from the head response")
        @MethodSource("dateGetters")
        @DisplayName("date getter")
        void lastModifiedTime(Function<S3BasicFileAttributes, FileTime> dateGetter) {
            var expectedFileTime = FileTime.from(Instant.parse("2023-11-07T08:29:12.847553Z"));
            assertThat(dateGetter.apply(attributes)).isEqualTo(expectedFileTime);
        }

        @Test
        @DisplayName("size() should return the contentLength from head response")
        void size() {
            assertThat(attributes.size()).isEqualTo(100L);
        }

        @Test
        @DisplayName("fileKey() should return the etag from head response")
        void fileKey() {
            assertThat(attributes.fileKey()).isEqualTo("someEtag");
        }

        @Test
        @DisplayName("isDirectory() should return false")
        void isDirectory() {
            assertThat(attributes.isDirectory()).isFalse();
        }

        @Test
        @DisplayName("isRegularFile() should return true")
        void isRegularFile() {
            assertThat(attributes.isRegularFile()).isTrue();
        }

        @Test
        @DisplayName("isSymbolicLink() should return false")
        void isSymbolicLink() {
            assertThat(attributes.isSymbolicLink()).isFalse();
        }

        @Test
        @DisplayName("isOther() should return false")
        void isOther() {
            assertThat(attributes.isOther()).isFalse();
        }

        private Stream<Arguments> dateGetters() {
            final Function<S3BasicFileAttributes, FileTime> lastModifiedTimeGetter = S3BasicFileAttributes::lastModifiedTime;
            final Function<S3BasicFileAttributes, FileTime> creationTimeGetter = S3BasicFileAttributes::creationTime;
            final Function<S3BasicFileAttributes, FileTime> lastAccessTimeGetter = S3BasicFileAttributes::lastAccessTime;
            return Stream.of(
                    Arguments.of(Named.of("lastModifiedTime", lastModifiedTimeGetter)),
                    Arguments.of(Named.of("creationTime", creationTimeGetter)),
                    Arguments.of(Named.of("lastAccessTime", lastAccessTimeGetter))
            );
        }
    }

    @Test
    @DisplayName("When a timeout happens getting attributes of a regular file, a RuntimeException should be thrown")
    void sizeOfFileThrowsWhenTimeout(){
        FileSystemProvider provider = mock();
        when(provider.getScheme()).thenReturn("s3");

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);
        when(fs.configuration()).thenReturn(new S3NioSpiConfiguration());

        S3AsyncClient mockClient = mock();
        when(fs.client()).thenReturn(mockClient);

        when(mockClient.headObject(anyConsumer())).thenReturn(
            CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(Duration.ofMinutes(1).toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return HeadObjectResponse.builder().build();
                }
            )
        );

        assertThatThrownBy(() -> S3BasicFileAttributes.get(S3Path.getPath(fs, "somefile"), Duration.ofMillis(1)))
            .isInstanceOf(IOException.class)
            .hasMessageContainingAll("operation timed out", "connectivity", "S3");
    }

}