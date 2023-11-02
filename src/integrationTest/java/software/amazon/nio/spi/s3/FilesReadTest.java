package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.BDDAssertions.then;
import static software.amazon.nio.spi.s3.Containers.localStackConnectionEndpoint;
import static software.amazon.nio.spi.s3.Containers.putObject;

@DisplayName("Files$read* should load file contents from localstack")
public class FilesReadTest
{
    private final Path path = Paths.get(URI.create(localStackConnectionEndpoint() + "/sink/files-read.txt"));

    @BeforeAll
    public static void createBucketAndFile(){
        Containers.createBucket("sink");
        putObject("sink", "files-read.txt", "some content");
    }

    @Test
    @DisplayName("when doing readAllBytes from existing file in s3")
    public void fileReadAllBytesShouldReturnFileContentsWhenFileFound() throws IOException {
        then(Files.readAllBytes(path)).isEqualTo("some content".getBytes());
    }

    @Test
    @DisplayName("when doing readAllLines from existing file in s3")
    public void fileReadAllLinesShouldReturnFileContentWhenFileFound() throws IOException {
        then(String.join("", Files.readAllLines(path))).isEqualTo("some content");
    }

}
