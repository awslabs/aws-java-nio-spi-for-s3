package examples;


import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class WalkFromRoot {

    /**
     * Walks a bucket from the root listing all directories and files. Internally uses the {@code S3FileSystemProvider}'s
     * {@code newDirectoryStream()} method which uses asynchronous paginated calls to S3.
     *
     * @param args provide a bucket name to walk
     * @throws IOException if a communication problem happens with the S3 service.
     */
    public static void main(String[] args) throws IOException {
        String bucketName = args[0];
        if (!bucketName.startsWith("s3:") && !bucketName.startsWith("s3x:")) {
            bucketName = "s3://" + bucketName;
        }
        final FileSystem s3 = FileSystems.newFileSystem(URI.create(bucketName), Collections.EMPTY_MAP);

        for (Path rootDir : s3.getRootDirectories()) {
            Files.walk(rootDir).forEach(System.out::println);
        }
    }
}
