package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.nio.spi.s3.Containers.localStackConnectionEndpoint;

@DisplayName("Files$newDirectoryStream()")
public class FilesDirectoryStreamTest {

    @Nested
    @DisplayName("should throw")
    class DirectoryDoesNotExist {

        @Test
        @DisplayName("when bucket does not exist")
        public void whenBucketNotFound() {
            final var path = Paths.get(URI.create(localStackConnectionEndpoint() + "/does-not-exist/some-directory"));
            assertThatThrownBy(() -> Files.newDirectoryStream(path, p -> true).iterator().hasNext())
                .isInstanceOf(NoSuchFileException.class);
        }
    }

}
