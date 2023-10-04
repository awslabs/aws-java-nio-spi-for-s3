package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.BDDAssertions.then;


public class S3FileSystemTest {
    @Test
    public void fileExistsShouldReturnFalseWhenBucketNotFound() {
        // We expect standard client to fail to locate the bucket
        final Path path = Paths.get(URI.create("s3x://" + Containers.localStackHost() + "/does-not-exists-" + System.currentTimeMillis() + "/dir"));
        then(path).isInstanceOf(S3Path.class);
        then(Files.exists(path)).isFalse();
    }
}
