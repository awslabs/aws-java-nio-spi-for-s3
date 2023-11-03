package software.amazon.nio.spi.examples;


import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class WalkFromRoot {

    /**
     * Walks a bucket from the root listing all directories and files. Internally uses the {@code S3FileSystemProvider}'s
     * {@code newDirectoryStream()} method which uses asynchronous paginated calls to S3.
     *
     * @param args provide a bucket name to walk
     * @throws IOException if a communication problem happens with the S3 service.
     */
    public static void main(String[] args) throws IOException {

        if (args.length < 1){
            System.err.println("Provide a bucket name to walk");
            System.exit(1);
        }

        String bucketName = args[0];
        if (!bucketName.startsWith("s3:") && !bucketName.startsWith("s3x:")) {
            bucketName = "s3://" + bucketName;
        }


        Path root = Paths.get(URI.create(bucketName));
        System.out.println("root.getClass() = " + root.getClass());
        
        FileSystem s3 = root.getFileSystem();

        for (Path rootDir : s3.getRootDirectories()) {
            try (Stream<Path> pathStream = Files.walk(rootDir)) {
                pathStream.forEach(System.out::println);
            }
        }

    }
}
