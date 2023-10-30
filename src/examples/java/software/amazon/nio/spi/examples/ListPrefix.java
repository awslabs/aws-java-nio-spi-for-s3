package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static java.util.Collections.*;

public class ListPrefix {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Provide an s3 prefix to list.");
            System.exit(1);
        }

        String prefix = args[0];
        try (final FileSystem fileSystem = FileSystems.newFileSystem(URI.create(prefix), emptyMap())) {
            Path s3Path = fileSystem.getPath(prefix);

            assert fileSystem.getClass().getName().contains("S3FileSystem");
            assert s3Path.getClass().getName().contains("S3Path");

            fileSystem.provider()
                    .newDirectoryStream(s3Path, item -> true)
                    .forEach(path -> System.out.println(path.getFileName()));
        }
    }
}
