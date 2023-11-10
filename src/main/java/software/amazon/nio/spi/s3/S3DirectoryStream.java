package software.amazon.nio.spi.s3;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;

public class S3DirectoryStream implements DirectoryStream<Path> {
    private final Iterator<Path> iterator;

    public S3DirectoryStream(Iterator<Path> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void close() {
    }

    @Override
    @NonNull
    public Iterator<Path> iterator() {
        return iterator;
    }

}