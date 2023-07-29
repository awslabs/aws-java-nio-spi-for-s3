/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.nio.spi.s3.util.TimeOutUtils;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Representation of {@link BasicFileAttributes} for an S3 object
 */
public class S3BasicFileAttributes implements BasicFileAttributes {

    private final S3Path path;
    private final S3AsyncClient client;
    private final String bucketName;

    private final Set<String> methodNamesToFilterOut =
            Stream.of("wait","toString","hashCode","getClass","notify","notifyAll").collect(Collectors.toSet());

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Constructor for the attributes of a path
     * @param path the path to represent the attributes of
     */
    protected S3BasicFileAttributes(S3Path path){
         this(path, path.getFileSystem().client());
    }

    /**
     * Constructor for the attributes of a path. A client is provided to perform any necessary S3 operations. This
     * method is suitable for Mocking by providing a Mocked client.
     * @param path the path to represent the attributes of
     * @param client the client to use for any S3 operations
     */
    protected S3BasicFileAttributes(S3Path path, S3AsyncClient client){
        this.path = path;
        this.client = client;
        bucketName = path.bucketName();
    }

    /**
     * Returns the time of last modification.
     *
     * <p> S3 "directories" do not support a time stamp
     * to indicate the time of last modification therefore this method returns a default value
     * representing the epoch (1970-01-01T00:00:00Z) as a proxy
     *
     * @return a {@code FileTime} representing the time the file was last
     * modified.
     * @throws RuntimeException if the S3Clients {@code RetryConditions} configuration was not able to handle the exception.
     */
    @Override
    public FileTime lastModifiedTime() {
        if(path.isDirectory()){
            return FileTime.from(Instant.EPOCH);
        }

        final Instant lastModified;
        try {
            lastModified = client.headObject(req -> req
                    .bucket(bucketName)
                    .key(path.getKey())
            ).get(TimeOutUtils.TIMEOUT_TIME_LENGTH_1, MINUTES).lastModified();
        } catch (ExecutionException e) {
            String errMsg = format("an '%s' error occurred while obtaining the last modified time of '%s' that was not handled successfully by the S3Client's configured RetryConditions", e.getCause().toString(), path.toUri());
            logger.error(errMsg);
            throw new RuntimeException(errMsg, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw TimeOutUtils.logAndGenerateExceptionOnTimeOut(logger, "lastModifiedTime()", TimeOutUtils.TIMEOUT_TIME_LENGTH_1, MINUTES);
        }
        return FileTime.from(lastModified);
    }

    /**
     * Returns the time of last access.
     * <p>Without enabling S3 server access logging, CloudTrail or similar it is not possible to obtain the access time
     * of an object, therefore the current implementation will return the @{code lastModifiedTime}</p>
     *
     * @return a {@code FileTime} representing the time of last access
     */
    @Override
    public FileTime lastAccessTime() {
        return lastModifiedTime();
    }

    /**
     * Returns the creation time. The creation time is the time that the file
     * was created.
     *
     * <p> Any modification of an S3 object results in a new Object so this time will be the same as
     * {@code lastModifiedTime}. A future implementation could consider times for versioned objects.
     *
     * @return a {@code FileTime} representing the time the file was created
     */
    @Override
    public FileTime creationTime() {
        return lastModifiedTime();
    }

    /**
     * Tells whether the file is a regular file with opaque content.
     *
     * @return {@code true} if the file is a regular file with opaque content
     */
    @Override
    public boolean isRegularFile() {
        return !path.isDirectory();
    }

    /**
     * Tells whether the file is a directory.
     *
     * @return {@code true} if the file is a directory
     */
    @Override
    public boolean isDirectory() {
        return path.isDirectory();
    }

    /**
     * Tells whether the file is a symbolic link.
     *
     * @return {@code false} always as S3 has no links
     */
    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    /**
     * Tells whether the file is something other than a regular file, directory,
     * or symbolic link. There are only objects in S3 and inferred directories
     *
     * @return {@code false} always
     */
    @Override
    public boolean isOther() {
        return false;
    }

    /**
     * Returns the size of the file (in bytes). The size may differ from the
     * actual size on the file system due to compression, support for sparse
     * files, or other reasons. The size of files that are not {@link
     * #isRegularFile regular} files is implementation specific and
     * therefore unspecified.
     *
     * @return the file size, in bytes
     * @throws RuntimeException if the S3Clients {@code RetryConditions} configuration was not able to handle the exception.
     */
    @Override
    public long size() throws RuntimeException{
        if(isDirectory()) return 0;

        try {
            return client.headObject(req -> req
                    .bucket(bucketName)
                    .key(path.getKey())
            ).get(TimeOutUtils.TIMEOUT_TIME_LENGTH_1, MINUTES).contentLength();
        } catch (ExecutionException e) {
            String errMsg = format("an '%s' error occurred while obtaining the size of '%s' that was not handled successfully by the S3Client's configured RetryConditions", e.getCause().toString(), path.toUri());
            logger.error(errMsg);
            throw new RuntimeException(errMsg, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException e){
            throw TimeOutUtils.logAndGenerateExceptionOnTimeOut(logger, "size()", TimeOutUtils.TIMEOUT_TIME_LENGTH_1, MINUTES);
        }
    }

    /**
     * Returns the S3 etag for the object
     *
     * @return the etag for an object, or {@code null} for a "directory"
     * @see Files#walkFileTree
     * @throws RuntimeException if the S3Clients {@code RetryConditions} configuration was not able to handle the exception.
     */
    @Override
    public Object fileKey() {
        if (path.isDirectory()) return null;

        try {
            return client.headObject(req -> req
                    .bucket(bucketName)
                    .key(path.toString())
            ).get(TimeOutUtils.TIMEOUT_TIME_LENGTH_1, MINUTES).eTag();
        } catch (ExecutionException e) {
            String errMsg = format("an '%s' error occurred while obtaining the file key of '%s' that was not handled successfully by the S3Client's configured RetryConditions", e.getCause().toString(), path.toUri());
            logger.error(errMsg);
            throw new RuntimeException(errMsg, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException e){
            throw TimeOutUtils.logAndGenerateExceptionOnTimeOut(logger, "size()", TimeOutUtils.TIMEOUT_TIME_LENGTH_1, MINUTES);
        }
    }

    /**
     * Construct a <code>Map</code> representation of this object
     * @return a map
     */
    protected Map<String, Object> asMap(){
        return asMap(x -> true);
    }

    /**
     * Construct a <code>Map</code> representation of this object with properties filtered
     * @param attributeFilter a filter to include properties in the resulting Map
     * @return a map filtered to only contain keys that pass the attributeFilter
     */
    protected Map<String, Object> asMap(Predicate<String> attributeFilter){
        HashMap<String, Object> map = new HashMap<>();
        Arrays.stream(this.getClass().getMethods())
                .filter(method -> method.getParameterCount() == 0)
                .filter(method -> !methodNamesToFilterOut.contains(method.getName()))
                .filter(method -> attributeFilter.test(method.getName()))
                .forEach(method -> {
                    logger.debug("method name: '{}'", method.getName());
                    try {
                        map.put(method.getName(), method.invoke(this));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        // should not ever happen as these are all public no arg methods
                        String errorMsg = format(
                                "an exception has occurred during a reflection operation on the methods of the file attributes of '%s', check if your Java SecurityManager is configured to allow reflection.", path.toUri());
                        logger.error("{}, caused by {}", errorMsg, e.getCause().getMessage());
                        throw new RuntimeException(errorMsg, e);
                    }
                });

        return map;
    }
}
