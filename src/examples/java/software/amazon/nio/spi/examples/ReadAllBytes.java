package software.amazon.nio.spi.examples;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ReadAllBytes {
    public static void main(String[] args) throws IOException {
        var filePath = Paths.get(URI.create(args[0]));

        final var bytes = Files.readAllBytes(filePath);
        // assumes this is a text file
        final var data = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(data);
    }
}
