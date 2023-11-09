package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.nio.spi.s3.Containers.localStackConnectionEndpoint;
import static software.amazon.nio.spi.s3.Containers.putObject;

@DisplayName("Files$write* should write file contents on s3 service")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilesWriteTest
{

    @BeforeAll
    public void createBucket() {
        Containers.createBucket("write-bucket");
    }

    @Test
    @DisplayName("append to an existing file")
    public void writeToExistingFile() throws IOException {
        putObject("write-bucket", "existing-file.txt", "some content");
        final Path path = Paths.get(URI.create(localStackConnectionEndpoint() + "/write-bucket/existing-file.txt"));

        Files.writeString(path, " more content", StandardOpenOption.APPEND);

        assertThat(path).hasContent("some content more content");
    }

    @Test
    @DisplayName("to a new file")
    public void writeToNewFile() throws IOException {
        final Path path = Paths.get(URI.create(localStackConnectionEndpoint() + "/write-bucket/new-file.txt"));

        Files.writeString(path, "content of new file", StandardOpenOption.CREATE_NEW);

        assertThat(path).hasContent("content of new file");
    }

}
