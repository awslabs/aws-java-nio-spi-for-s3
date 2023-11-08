package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class ListPrefix {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Provide an s3 prefix to list.");
            System.exit(1);
        }

        var s3Path = Paths.get(URI.create(args[0]));

        // if the prefix doesn't exist you won't get any listings
        System.err.println("Files.exists(s3Path) = " + Files.exists(s3Path));

        //if it's not a directory you won't get any listings.
        System.err.println("Files.isDirectory(s3Path) = " + Files.isDirectory(s3Path));

        try (var listed = Files.list(s3Path)) {
            listed.forEach(System.out::println);
        }
    }
}
