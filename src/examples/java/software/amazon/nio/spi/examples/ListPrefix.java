package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ListPrefix {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Provide an s3 prefix to list.");
            System.exit(1);
        }

        String prefix = args[0];

        Path s3Path = Paths.get(URI.create(prefix));

        assert s3Path.getClass().getName().contains("S3Path");

        s3Path.getFileSystem().provider()
                .newDirectoryStream(s3Path, item -> true)
                .forEach(path -> System.out.println(path.getFileName()));

    }
}
