package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("S3BasicFileAttributes")
class S3BasicFileAttributesTest {

    @Nested
    @DisplayName("directory")
    class Directories {

        @Test
        @DisplayName("lastModifiedTime() should return epoch")
        void lastModifiedTime() {
            S3FileSystem fs = mock();
            FileSystemProvider provider = mock();
            when(fs.provider()).thenReturn(provider);
            when(fs.configuration()).thenReturn(new S3NioSpiConfiguration());
            when(provider.getScheme()).thenReturn("s3");

            S3Path directory = S3Path.getPath(fs, "s3://somebucket/somedirectory/");
            S3BasicFileAttributes s3BasicFileAttributes = new S3BasicFileAttributes(directory);
            assertThat(s3BasicFileAttributes.lastModifiedTime()).isEqualTo(FileTime.from(Instant.EPOCH));
        }
    }
}