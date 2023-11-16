/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static software.amazon.nio.spi.s3.Constants.PATH_SEPARATOR;

import io.reactivex.rxjava3.core.Flowable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Iterator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

class S3DirectoryStream implements DirectoryStream<Path> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final Iterator<Path> iterator;

    S3DirectoryStream(S3FileSystem fs, String bucketName, String finalDirName, Filter<? super Path> filter) {
        final var listObjectsV2Publisher = fs.client().listObjectsV2Paginator(req -> req
            .bucket(bucketName)
            .prefix(finalDirName)
            .delimiter(PATH_SEPARATOR));

        iterator = pathIteratorForPublisher(filter, fs, finalDirName, listObjectsV2Publisher);
        //noinspection ResultOfMethodCallIgnored
        iterator.hasNext();
    }

    @Override
    @NonNull
    public Iterator<Path> iterator() {
        return iterator;
    }

    @Override
    public void close() {
    }

    /**
     * Get an iterator for a {@code ListObjectsV2Publisher}
     *
     * @param filter                 a filter to apply to returned Paths. Only accepted paths will be included.
     * @param fs                     the Filesystem.
     * @param finalDirName           the directory name that will be streamed.
     * @param listObjectsV2Publisher the publisher that returns objects and common prefixes that are iterated on.
     * @return an iterator for {@code Path}s constructed from the {@code ListObjectsV2Publisher}s responses.
     */
    private Iterator<Path> pathIteratorForPublisher(
        final DirectoryStream.Filter<? super Path> filter,
        final FileSystem fs, String finalDirName,
        final ListObjectsV2Publisher listObjectsV2Publisher) {
        final var prefixPublisher = listObjectsV2Publisher.commonPrefixes().map(CommonPrefix::prefix);
        final var keysPublisher = listObjectsV2Publisher.contents().map(S3Object::key);

        return Flowable.concat(prefixPublisher, keysPublisher)
            .map(fs::getPath)
            .filter(path -> !isEqualToParent(finalDirName, path))  // including the parent will induce loops
            .filter(path -> tryAccept(filter, path))
            .blockingStream()
            .iterator();
    }


    private static boolean isEqualToParent(String finalDirName, Path p) {
        return ((S3Path) p).getKey().equals(finalDirName);
    }

    private boolean tryAccept(DirectoryStream.Filter<? super Path> filter, Path path) {
        try {
            return filter.accept(path);
        } catch (IOException e) {
            logger.warn("An IOException was thrown while filtering the path: {}." +
                " Set log level to debug to show stack trace", path);
            logger.debug(e.getMessage(), e);
            return false;
        }
    }
}