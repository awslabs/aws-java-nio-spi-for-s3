package software.amazon.nio.spi.examples;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class CopyToS3 {
    /**
     * Demonstrates a cross filesystem operation by copying a local (temp) file into an S3 bucket.
     * @param args the s3 path (URI) to copy to
     * @throws IOException if the temp file cannot be created or a communication problem happens with the S3 service.
     */
    public static void main(String[] args) throws IOException {
        Path s3Path = Paths.get(URI.create(args[0]));

        String content = "test";

        final Path tempFile = Files.createTempFile("test", "tmp");
        tempFile.toFile().deleteOnExit();
        try (FileWriter writer = new FileWriter(tempFile.toFile())) {
            writer.write(content);
        }

        // Remove the copy option to prevent replacement of an existing file.
        Files.copy(tempFile, s3Path, StandardCopyOption.REPLACE_EXISTING);
    }
}
