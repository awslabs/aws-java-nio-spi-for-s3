package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.*;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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


    }
}