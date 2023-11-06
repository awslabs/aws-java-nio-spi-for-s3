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
            System.out.println("Provide an s3 prefix to list.");
            System.exit(1);
        }

        Path s3Path = Paths.get(URI.create(args[0]));

        try (Stream<Path> listed = Files.list(s3Path)) {
            listed.forEach(System.out::println);
        }
    }
}
