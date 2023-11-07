package software.amazon.nio.spi.s3;

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

import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static software.amazon.nio.spi.s3.S3Matchers.anyConsumer;

@DisplayName("S3BasicFileAttributes")
public class S3BasicFileAttributesTest {

    private static final FileTime EPOCH_FILE_TIME = FileTime.from(Instant.EPOCH);

    @Nested
    @DisplayName("directory")
    class Directories {

        private S3BasicFileAttributes directoryAttributes;

        @BeforeEach
        void configureDirectory(){
            S3FileSystem fs = mock();
            FileSystemProvider provider = mock();
            when(fs.provider()).thenReturn(provider);
            when(fs.configuration()).thenReturn(new S3NioSpiConfiguration());
            when(provider.getScheme()).thenReturn("s3");

            S3Path directory = S3Path.getPath(fs, "s3://somebucket/somedirectory/");
            directoryAttributes = new S3BasicFileAttributes(directory);
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

        @BeforeEach
        void configureRegularFile() {
            S3FileSystem fs = mock();

            FileSystemProvider provider = mock();
            when(fs.provider()).thenReturn(provider);
            when(provider.getScheme()).thenReturn("s3");

            when(fs.configuration()).thenReturn(new S3NioSpiConfiguration());

            when(fs.client()).thenReturn(mockClient);

            S3Path file = S3Path.getPath(fs, "s3://somebucket/somefile");
            attributes = new S3BasicFileAttributes(file);
            reset(mockClient);
        }

        @ParameterizedTest(name = "{0} should return the Instant from the head response")
        @MethodSource("dateGetters")
        @DisplayName("date getter")
        void lastModifiedTime(Function<S3BasicFileAttributes, FileTime> dateGetter) {
            when(mockClient.headObject(anyConsumer())).thenReturn(
                    CompletableFuture.supplyAsync(() ->
                            HeadObjectResponse.builder().lastModified(Instant.parse("2023-11-07T08:29:12.847553Z")).build()
                    )
            );

            FileTime expectedFileTime = FileTime.from(Instant.parse("2023-11-07T08:29:12.847553Z"));
            assertThat(dateGetter.apply(attributes)).isEqualTo(expectedFileTime);
        }

        @Test
        @DisplayName("size() should return the contentLength from head response")
        void size() {
            when(mockClient.headObject(anyConsumer())).thenReturn(
                    CompletableFuture.supplyAsync(() ->
                            HeadObjectResponse.builder().contentLength(100L).build()
                    )
            );

            assertThat(attributes.size()).isEqualTo(100L);
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

}