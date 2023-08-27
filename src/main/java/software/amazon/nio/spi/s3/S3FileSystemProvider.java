/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import io.reactivex.rxjava3.core.Flowable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CopyRequest;
import software.amazon.nio.spi.s3.util.TimeOutUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import java.util.HashMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static software.amazon.awssdk.http.HttpStatusCode.FORBIDDEN;
import static software.amazon.awssdk.http.HttpStatusCode.NOT_FOUND;
import static software.amazon.nio.spi.s3.util.TimeOutUtils.TIMEOUT_TIME_LENGTH_1;
import static software.amazon.nio.spi.s3.util.TimeOutUtils.logAndGenerateExceptionOnTimeOut;

/**
 * Service-provider class for S3 when represented as an NIO filesystem. The methods defined by the Files class will
 * delegate to an instance of this class when referring to an object in S3. This class will in turn make calls to the
 * S3 service.
 * <br>
 * This class should never be used directly. It is invoked by the service loader when, for example, the java.nio.file.Files
 * class is used to address an object beginning with the scheme "s3".
 */
public class S3FileSystemProvider extends FileSystemProvider {

    /**
     * Constant for the S3 scheme "s3"
     */
    public static final String SCHEME = "s3";

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static Map<String, S3FileSystem> cache = new HashMap<>();

    /**
     * Returns the URI scheme that identifies this provider.
     *
     * @return The URI scheme (s3)
     */
    @Override
    public String getScheme() {
        return SCHEME;
    }

    /**
     * Constructs a new {@code FileSystem} object identified by a URI. This
     * method is invoked by the {@link FileSystems#newFileSystem(URI, Map)}
     * method to open a new file system identified by a URI.
     *
     * <p> The {@code uri} parameter is an absolute, hierarchical URI, with a
     * scheme equal (without regard to case) to the scheme supported by this
     * provider. The exact form of the URI is highly provider dependent. The
     * {@code env} parameter is a map of provider specific properties to configure
     * the file system.
     *
     * <p> This method throws {@link FileSystemAlreadyExistsException} if the
     * file system already exists because it was previously created by an
     * invocation of this method. Once a file system is {@link
     * FileSystem#close closed} it is provider-dependent if the
     * provider allows a new file system to be created with the same URI as a
     * file system it previously created.
     *
     * @param uri URI reference
     * @param env A map of provider specific properties to configure the file system;
     *            may be empty
     * @return A new file system
     *
     * @throws FileSystemAlreadyExistsException if the file system has already been created
     * @throws IllegalArgumentException if the pre-conditions for the uri parameter are not met, or the env parameter does not contain properties required by the provider, or a property value is invalid
     */
    @Override
    public S3FileSystem newFileSystem(URI uri, Map<String, ?> env)
    throws FileSystemAlreadyExistsException {

        if (uri == null) {
            throw new IllegalArgumentException("uri can not be null");
        }
        if (uri.getScheme() == null) {
            throw new IllegalArgumentException(
                String.format("invalid uri '%s', please provide an uri as s3://bucket", uri.toString())
            );
        }
        if (uri.getAuthority() == null) {
            throw new IllegalArgumentException(
                String.format("invalid uri '%s', please provide an uri as s3://bucket", uri.toString())
            );
        }

        S3FileSystem fs = null;

        String key = getFileSystemKey(uri);
        if (cache.containsKey(key)) {
            throw new FileSystemAlreadyExistsException("a file system already exists for '" + key + "', use getFileSystem() instead");
        }
        cache.put(key, fs = new S3FileSystem(uri, this));

        return fs;
    }

    /**
     * Same as newFileSystem(uri, Collections.EMPTY_MAP);
     *
     * @param uri URI reference
     *
     * @return newFileSystem(uri, Collections.EMPTY_MAP)
     *
     * @throws FileSystemAlreadyExistsException if the file system has already been created
     * @throws IllegalArgumentException if the pre-conditions for the uri parameter
     *         are not met, or the env parameter does not contain properties
     *         required by the provider, or a property value is invalid
     */
    public S3FileSystem newFileSystem(URI uri) {
        return newFileSystem(uri, Collections.EMPTY_MAP);
    }

    /**
     * Returns an existing {@code FileSystem} created by this provider.
     *
     * <p> This method returns a reference to a {@code FileSystem} that was
     * created by invoking the {@link #newFileSystem(URI, Map) newFileSystem(URI,Map)}
     * method. File systems created the {@link #newFileSystem(Path, Map)
     * newFileSystem(Path,Map)} method are not returned by this method.
     * The file system is identified by its {@code URI}. Its exact form
     * is highly provider dependent. In the case of the default provider the URI's
     * path component is {@code "/"} and the authority, query and fragment components
     * are undefined (Undefined components are represented by {@code null}).
     *
     * <p> Once a file system created by this provider is {@link
     * FileSystem#close closed} it is provider-dependent if this
     * method returns a reference to the closed file system or throws {@link
     * FileSystemNotFoundException}. If the provider allows a new file system to
     * be created with the same URI as a file system it previously created then
     * this method throws the exception if invoked after the file system is
     * closed (and before a new instance is created by the {@link #newFileSystem
     * newFileSystem} method).
     *
     * <p> If a security manager is installed then a provider implementation
     * may require to check a permission before returning a reference to an
     * existing file system. In the case of the {@link FileSystems#getDefault
     * default} file system, no permission check is required.
     *
     * @param uri URI reference
     * @return The file system
     * @throws IllegalArgumentException    If the pre-conditions for the {@code uri} parameter aren't met
     * @throws FileSystemNotFoundException If the file system does not exist
     * @throws SecurityException           If a security manager is installed, and it denies an unspecified
     *                                     permission.
     */
    @Override
    public S3FileSystem getFileSystem(URI uri) {
        return getFileSystem(uri, false);
    }

    /**
     * Similar to getFileSystem(uri), but it allows to create the file system if
     * not yet created.
     *
     * @param uri URI reference
     * @param create if true, the file system is created if not already done
     *
     * @return The file system
     *
     * @throws IllegalArgumentException    If the pre-conditions for the {@code uri} parameter aren't met
     * @throws FileSystemNotFoundException If the file system does not exist
     * @throws SecurityException           If a security manager is installed, and it denies an unspecified
     *                                     permission.
     */
    protected S3FileSystem getFileSystem(URI uri, boolean create) {
        String key = getFileSystemKey(uri);
        S3FileSystem fs = cache.get(key);

        if (fs == null) {
            if (!create) {
                throw new FileSystemNotFoundException("file system not found for '" + key + "'");
            }
            fs = newFileSystem(uri);
        }

        return fs;
    }

    public void closeFileSystem(S3FileSystem fs) {
        for (String key: cache.keySet()) {
            if (fs == cache.get(key)) {
                cache.remove(key); return;
            }
        }
    }

    /**
     * Return a {@code Path} object by converting the given {@link URI}. The
     * resulting {@code Path} is associated with a {@link FileSystem} that
     * already exists or is constructed automatically.
     *
     * <p> The exact form of the URI is file system provider dependent. In the
     * case of the default provider, the URI scheme is {@code "file"} and the
     * given URI has a non-empty path component, and undefined query, and
     * fragment components. The resulting {@code Path} is associated with the
     * default {@link FileSystems#getDefault default} {@code FileSystem}.
     *
     * <p> If a security manager is installed then a provider implementation
     * may require to check a permission. In the case of the {@link
     * FileSystems#getDefault default} file system, no permission check is
     * required.
     *
     * @param uri The URI to convert. Must not be null.
     * @return The resulting {@code Path}
     * @throws IllegalArgumentException    If the URI scheme does not identify this provider or other
     *                                     preconditions on the uri parameter do not hold
     * @throws FileSystemNotFoundException The file system, identified by the URI, does not exist and
     *                                     cannot be created automatically
     * @throws SecurityException           If a security manager is installed, and it denies an unspecified
     *                                     permission.
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public S3Path getPath(URI uri)
    throws IllegalArgumentException, FileSystemNotFoundException, SecurityException {
        Objects.requireNonNull(uri);
        return getFileSystem(uri, true).getPath(uri.getScheme() + ":/" + uri.getPath());
    }

    /**
     * Opens or creates a file, returning a seekable byte channel to access the
     * file. This method works in exactly the manner specified by the {@link
     * Files#newByteChannel(Path, Set, FileAttribute[])} method.
     *
     * @param path    the path to the file to open or create
     * @param options options specifying how the file is opened
     * @param attrs   an optional list of file attributes to set atomically when
     *                creating the file
     * @return a new seekable byte channel
     * @throws IllegalArgumentException      if the set contains an invalid combination of options
     * @throws UnsupportedOperationException if an unsupported open option is specified or the array contains
     *                                       attributes that cannot be set atomically when creating the file
     * @throws FileAlreadyExistsException    if a file of that name already exists and the {@link
     *                                       StandardOpenOption#CREATE_NEW CREATE_NEW} option is specified
     *                                       <i>(optional specific exception)</i>
     * @throws IOException                   if an I/O error occurs
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                                       method is invoked to check read access to the path if the file is
     *                                       opened for reading. The {@link SecurityManager#checkWrite(String)
     *                                       checkWrite} method is invoked to check write access to the path
     *                                       if the file is opened for writing. The {@link
     *                                       SecurityManager#checkDelete(String) checkDelete} method is
     *                                       invoked to check delete access if the file is opened with the
     *                                       {@code DELETE_ON_CLOSE} option.
     */
    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        return this.newByteChannel(null, path, options, attrs);
    }

    /**
     * Construct a byte channel for the path with the specified client. A more composable and testable (by using a Mock Client)
     * version of the public method
     * @param client The client to use for the channel
     * @param path The path to open
     * @param options The options to use
     * @param attrs The attributes to use
     * @return A new byte channel for the object at {@code path}
     * @throws IOException If the channel could not be created
     */
    protected SeekableByteChannel newByteChannel(S3AsyncClient client, Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        if (Objects.isNull(options)) {
            options = Collections.emptySet();
        }

        final S3Path s3Path = (S3Path)path;
        final S3SeekableByteChannel channel;
        final S3FileSystem fs = s3Path.getFileSystem();

        channel = new S3SeekableByteChannel(s3Path, fs.client(), options);
        fs.registerOpenChannel(channel);

        return channel;
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path dir = ...;
     *             S3AsyncClient s3 = ...;
     *             DirectoryStream filter = ...;
     *
     *             p.newDirectoryStream(s3, path, filter);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path dir = ...;
     *
     *             p.newDirectoryStreampath, filter);
     *             </pre>
     */
    @Deprecated
    protected DirectoryStream<Path> newDirectoryStream(S3AsyncClient s3Client, Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException, ExecutionException, InterruptedException {
        return newDirectoryStream(dir, filter);
    }

    /**
     * Opens a directory, returning a {@code DirectoryStream} to iterate over
     * the entries in the directory. This method works in exactly the manner
     * specified by the {@link
     * Files#newDirectoryStream(Path, DirectoryStream.Filter)}
     * method.
     *
     * @param dir    the path to the directory
     * @param filter the directory stream filter
     * @return a new and open {@code DirectoryStream} object
     * @throws IOException if the stream cannot be created or has a streaming problem.
     */
    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        S3Path s3Path = (S3Path) dir;

        String dirName = s3Path.toAbsolutePath().getKey();

        if (!s3Path.isDirectory()) {
            dirName = dirName + S3Path.PATH_SEPARATOR;
        }

        final String bucketName = s3Path.bucketName();
        final FileSystem fs = s3Path.getFileSystem();
        final String finalDirName = dirName;

        final ListObjectsV2Publisher listObjectsV2Publisher = s3Path.getFileSystem().client().listObjectsV2Paginator(req -> req
                .bucket(bucketName)
                .prefix(finalDirName)
                .delimiter(S3Path.PATH_SEPARATOR));

        final Iterator<Path> iterator = pathIteratorForPublisher(filter, fs, finalDirName, listObjectsV2Publisher);

        return new DirectoryStream<Path>() {
            @Override
            public void close() {
            }

            @Override
            public Iterator<Path> iterator() {
                return iterator;
            }
        };
    }

    /**
     * Get an iterator for a {@code ListObjectsV2Publisher}. This method is protected level access only for testing
     * purposes. It is not intended to be used by any other code outside of this class.
     * @param filter a filter to apply to returned Paths. Only accepted paths will be included.
     * @param fs the Filesystem.
     * @param finalDirName the directory name that will be streamed.
     * @param listObjectsV2Publisher the publisher that returns objects and common prefixes that are iterated on.
     * @return an iterator for {@code Path}s constructed from the {@code ListObjectsV2Publisher}s responses.
     */
    private Iterator<Path> pathIteratorForPublisher (
            final DirectoryStream.Filter<? super Path> filter,
            final FileSystem fs, String finalDirName,
            final ListObjectsV2Publisher listObjectsV2Publisher) {
        return Flowable.fromPublisher(listObjectsV2Publisher)
                .flatMapIterable(response -> {

                    //add common prefixes from this page
                    List<String> items = response
                            .commonPrefixes().stream()
                            .map(CommonPrefix::prefix)
                            .collect(Collectors.toList());

                    //add s3 objects from this page
                    items.addAll(response
                            .contents().stream()
                            .map(S3Object::key)
                            .collect(Collectors.toList()));

                    // convert to S3Path and apply directory stream filter
                    return items.stream()
                            .filter(p -> !((S3Path)fs.getPath(p)).getKey().equals(finalDirName))  // including the parent will induce loops
                            .map(fs::getPath)
                            .filter(path -> {
                                try {
                                    return filter.accept(path);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    return false;
                                }
                            }).collect(Collectors.toList());
                })
                .blockingStream()
                .map(Path.class::cast) // upcast to Path from S3Path
                .iterator();
    }

    /**
     * Creates a new directory. This method works in exactly the manner
     * specified by the {@link Files#createDirectory} method.
     *
     * @param dir   the directory to create
     * @param attrs an optional list of file attributes to set atomically when
     *              creating the directory
     */
    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        S3FileSystem fs = ((S3Path)dir).getFileSystem();
        try {
            String directoryKey = ((S3Path)dir).toRealPath(NOFOLLOW_LINKS).getKey();
            if (!directoryKey.endsWith(S3Path.PATH_SEPARATOR) && !directoryKey.isEmpty()) {
                directoryKey = directoryKey + S3Path.PATH_SEPARATOR;
            }

            long timeOut = TIMEOUT_TIME_LENGTH_1;
            final TimeUnit unit = MINUTES;
            try {
                fs.client().putObject(PutObjectRequest.builder()
                                        .bucket(fs.bucketName())
                                        .key(directoryKey)
                                        .build(),
                                AsyncRequestBody.empty())
                        .get(timeOut, unit);
            } catch (TimeoutException e) {
                throw logAndGenerateExceptionOnTimeOut(logger, "createDirectory", timeOut, unit);
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path dir = ...;
     *             S3AsyncClient s3 = ...;
     *
     *             p.createDirectory(s3, dir);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path dir = ...;
     *
     *             p.createDirectory(source, target);
     *             </pre>
     */
     @Deprecated
     protected void createDirectory(S3AsyncClient s3Client, Path dir, FileAttribute<?>... attrs) throws IOException, ExecutionException, InterruptedException {
         createDirectory(forceAwsClient(dir, s3Client), attrs);
     }

    /**
     * Deletes a file. This method works in exactly the  manner specified by the
     * {@link Files#delete} method.
     *
     * @param path the path to the file to delete
     */
    @Override
    public void delete(Path path) throws IOException {
        S3FileSystem fs = ((S3Path)path).getFileSystem();
        try {
            final S3Path s3Path = (S3Path) path;
            final S3AsyncClient s3Client = fs.client();
            final String bucketName = fs.bucketName();

            final String prefix = s3Path.toRealPath(NOFOLLOW_LINKS).getKey();

            long timeOut = TIMEOUT_TIME_LENGTH_1;
            final TimeUnit unit = MINUTES;
            try {
                List<List<ObjectIdentifier>> keys = getContainedObjectBatches(s3Client, bucketName, prefix, timeOut, unit);

                for (List<ObjectIdentifier> keyList : keys) {
                    s3Client.deleteObjects(DeleteObjectsRequest.builder()
                                    .bucket(bucketName)
                                    .delete(Delete.builder()
                                            .objects(keyList)
                                            .build())
                                    .build())
                            .get(timeOut, unit);
                }
            } catch (TimeoutException e) {
                throw logAndGenerateExceptionOnTimeOut(logger, "delete", timeOut, unit);
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path path = ...;
     *             S3AsyncClient s3 = ...;
     *
     *             p.delete(s3, path);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path path = ...;
     *
     *             p.delete(path);
     *             </pre>
     */
    @Deprecated
    protected void delete(S3AsyncClient s3Client, Path path) throws IOException, ExecutionException, InterruptedException {
        delete(forceAwsClient(path, s3Client));
    }



    /**
     * Copy a file to a target file. This method works in exactly the manner
     * specified by the {@link Files#copy(Path, Path, CopyOption[])} method
     * except that both the source and target paths must be associated with
     * this provider.
     *
     * @param source  the path to the file to copy
     * @param target  the path to the target file
     * @param options options specifying how the copy should be done
     */
    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        //
        // TODO: source and target can belong to any file system (confirmed, see
        //       https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/135),
        //       we can not assume they points to S3 objects
        //
        try {
            // If both paths point to the same object, this is a no-op
            if (source.equals(target)) {
                return;
            }

            List<CopyOption> copyOptions = Arrays.asList(options);

            S3Path s3SourcePath = (S3Path) source;
            S3Path s3TargetPath = (S3Path) target;

            final S3FileSystem fs = ((S3Path)source).getFileSystem();
            final S3AsyncClient s3Client = fs.client();
            final String bucketName = fs.bucketName();

            String prefix = s3SourcePath.toRealPath(NOFOLLOW_LINKS).getKey();

            long timeOut = TIMEOUT_TIME_LENGTH_1;
            final TimeUnit unit = MINUTES;
            try {
                List<List<ObjectIdentifier>> keys = getContainedObjectBatches(s3Client, bucketName, prefix, timeOut, unit);

                for (List<ObjectIdentifier> keyList : keys) {
                    for (ObjectIdentifier objectIdentifier : keyList) {
                        S3Path resolvedS3TargetPath = s3TargetPath.resolve(objectIdentifier.key().replaceFirst(prefix + S3Path.PATH_SEPARATOR, ""));

                        if (!copyOptions.contains(StandardCopyOption.REPLACE_EXISTING) && exists(s3Client, resolvedS3TargetPath)) {
                            throw new FileAlreadyExistsException("File already exists at the target key");
                        }

                        try (S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3Client).build()) {
                            s3TransferManager.copy(CopyRequest.builder()
                                    .copyObjectRequest(CopyObjectRequest.builder()
                                            .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                                            .sourceBucket(bucketName)
                                            .sourceKey(objectIdentifier.key())
                                            .destinationBucket(resolvedS3TargetPath.bucketName())
                                            .destinationKey(resolvedS3TargetPath.getKey())
                                            .build())
                                    .build()).completionFuture().get(timeOut, unit);
                        }
                    }
                }
            } catch (TimeoutException e) {
                throw logAndGenerateExceptionOnTimeOut(logger, "copy", timeOut, unit);
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path source = ..., target = ,,,;
     *             S3AsyncClient s3 = ...;
     *
     *             p.copy(s3, source, target);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path source = ..., target = ,,,;
     *
     *             p.copy(source, target);
     *             </pre>
     */
    @Deprecated
    protected void copy(S3AsyncClient s3Client, Path source, Path target, CopyOption... options) throws IOException, ExecutionException, InterruptedException {
        copy(forceAwsClient((S3Path)source, s3Client), target, options);
    }

    protected boolean exists(S3AsyncClient s3Client, S3Path path) throws InterruptedException, TimeoutException {
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(path.bucketName()).key(path.getKey()).build())
                    .get(TIMEOUT_TIME_LENGTH_1, MINUTES);
            return true;
        } catch (ExecutionException | NoSuchKeyException e) {
            logger.debug("Could not retrieve object head information", e);
            return false;
        }
    }

    /**
     * Move or rename a file to a target file. This method works in exactly the
     * manner specified by the {@link Files#move} method except that both the
     * source and target paths must be associated with this provider.
     *
     * @param source  the path to the file to move
     * @param target  the path to the target file
     * @param options options specifying how the move should be done
     */
    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        this.copy(source, target, options);
        this.delete(source);
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path source = ..., target = ,,,;
     *             S3AsyncClient s3 = ...;
     *
     *             p.move(s3, source, target);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path source = ..., target = ,,,;
     *
     *             p.move(source, target);
     *             </pre>
     */
    @Deprecated
    protected void move(S3AsyncClient s3Client, Path source, Path target, CopyOption... options) throws IOException, ExecutionException, InterruptedException {
        move(forceAwsClient(source, s3Client), target, options);
    }

    /**
     * Tests if two paths locate the same file. This method works in exactly the
     * manner specified by the {@link Files#isSameFile} method.
     *
     * @param path  one path to the file
     * @param path2 the other path
     * @return {@code true} if, and only if, the two paths locate the same file
     * @throws IOException       if an I/O error occurs
     * @throws SecurityException In the case of the default provider, and a security manager is
     *                           installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                           method is invoked to check read access to both files.
     */
    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return path.toRealPath(NOFOLLOW_LINKS).equals(path2.toRealPath(NOFOLLOW_LINKS));
    }

    /**
     * There are no hidden files in S3
     *
     * @param path the path to the file to test
     * @return {@code false} always
     */
    @Override
    public boolean isHidden(Path path) {
        return false;
    }

    /**
     * S3 buckets don't have partitions or volumes so there are no file stores
     *
     * @param path the path to the file
     * @return {@code null} always
     */
    @Override
    public FileStore getFileStore(Path path) {
        return null;
    }

    /**
     * Checks the existence, and optionally the accessibility, of a file.
     *
     * <p> This method may be used by the {@link Files#isReadable isReadable},
     * {@link Files#isWritable isWritable} and {@link Files#isExecutable
     * isExecutable} methods to check the accessibility of a file.
     *
     * <p> This method checks the existence of a file and that this Java virtual
     * machine has appropriate privileges that would allow it to access the file
     * according to all the access modes specified in the {@code modes} parameter
     * as follows:
     * <br>
     * <table class="striped">
     * <caption style="display:none">Access Modes</caption>
     * <thead>
     * <tr> <th scope="col">Value</th> <th scope="col">Description</th> </tr>
     * </thead>
     * <tbody>
     * <tr>
     *   <th scope="row"> {@link AccessMode#READ READ} </th>
     *   <td> Checks that the file exists and that the Java virtual machine has
     *     permission to read the file. </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link AccessMode#WRITE WRITE} </th>
     *   <td> Checks that the file exists and that the Java virtual machine has
     *     permission to write to the file, </td>
     * </tr>
     * <tr>
     *   <th scope="row"> {@link AccessMode#EXECUTE EXECUTE} </th>
     *   <td> Checks that the file exists and that the Java virtual machine has
     *     permission to {@link Runtime#exec execute} the file. The semantics
     *     may differ when checking access to a directory. For example, on UNIX
     *     systems, checking for {@code EXECUTE} access checks that the Java
     *     virtual machine has permission to search the directory in order to
     *     access file or subdirectories. </td>
     * </tr>
     * </tbody>
     * </table>
     *
     * <p> If the {@code modes} parameter is of length zero, then the existence
     * of the file is checked.
     *
     * <p> This method follows symbolic links if the file referenced by this
     * object is a symbolic link. Depending on the implementation, this method
     * may require reading file permissions, access control lists, or other
     * file attributes in order to check the effective access to the file. To
     * determine the effective access to a file may require access to several
     * attributes and so in some implementations this method may not be atomic
     * with respect to other file system operations.
     *
     * @param path  the path to the file to check
     * @param modes The access modes to check; may have zero elements
     * @throws UnsupportedOperationException an implementation is required to support checking for
     *                                       {@code READ}, {@code WRITE}, and {@code EXECUTE} access. This
     *                                       exception is specified to allow for the {@code Access} enum to
     *                                       be extended in future releases.
     * @throws NoSuchFileException           if a file does not exist <i>(optional specific exception)</i>
     * @throws AccessDeniedException         the requested access would be denied or the access cannot be
     *                                       determined because the Java virtual machine has insufficient
     *                                       privileges or other reasons. <i>(optional specific exception)</i>
     * @throws IOException                   if an I/O error occurs
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, the {@link SecurityManager#checkRead(String) checkRead}
     *                                       is invoked when checking read access to the file or only the
     *                                       existence of the file, the {@link SecurityManager#checkWrite(String)
     *                                       checkWrite} is invoked when checking write access to the file,
     *                                       and {@link SecurityManager#checkExec(String) checkExec} is invoked
     *                                       when checking execute access.
     */
    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        try {
            assert path instanceof S3Path;

            final S3Path s3Path = (S3Path) path.toRealPath(NOFOLLOW_LINKS);
            final S3FileSystem fs = s3Path.getFileSystem();
            final String bucketName = fs.bucketName();
            final S3AsyncClient s3Client = fs.client();

            final CompletableFuture<? extends S3Response> response;
            if (s3Path.equals(s3Path.getRoot())) {
                response = s3Client.headBucket(request -> request.bucket(bucketName));
            } else {
                response = s3Client.headObject(req -> req.bucket(bucketName).key(s3Path.getKey()));
            }

            long timeOut = TimeOutUtils.TIMEOUT_TIME_LENGTH_1;
            TimeUnit unit = MINUTES;

            try {
                SdkHttpResponse httpResponse = response.get(timeOut, unit).sdkHttpResponse();
                if (httpResponse.isSuccessful()) return;

                //
                // TODO: it looks like if status is not SUCCESS an ExecutionException
                // is thrown, therefore the following error handling is never
                // triggered
                //

                if (httpResponse.statusCode() == FORBIDDEN)
                    throw new AccessDeniedException(s3Path.toString());

                if (httpResponse.statusCode() == NOT_FOUND)
                    throw new NoSuchFileException(s3Path.toString());

                throw new IOException(String.format("exception occurred while checking access, response code was '%d'",
                        httpResponse.statusCode()));

            } catch (TimeoutException e) {
                throw logAndGenerateExceptionOnTimeOut(logger, "checkAccess", timeOut, unit);
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path path = ...;
     *             S3AsyncClient s3 = ...;
     *
     *             p.checkAccess(s3, dir);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path path = ...;
     *
     *             p.checkAccess(path);
     *             </pre>
     */
    @Deprecated
    protected void checkAccess(S3AsyncClient s3Client, Path path, AccessMode... modes) throws IOException, ExecutionException, InterruptedException {
        checkAccess(forceAwsClient(path, s3Client), modes);
    }

    /**
     * Returns a file attribute view of a given type. This method works in
     * exactly the manner specified by the {@link Files#getFileAttributeView}
     * method.
     *
     * @param <V>
     * @param path    the path to the file
     * @param type    the {@code Class} object corresponding to the file attribute view.
     *                Must be {@code BasicFileAttributeView.class} or {@code S3FileAttributeView.class}
     * @param options ignored as there are no links in S3
     * @return a file attribute view of the specified type, or {@code null} if
     * the attribute view type is not available
     */
    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        Objects.requireNonNull(path, "cannot obtain attributes for a null path");
        Objects.requireNonNull(type, "the type of attribute view required cannot be null");

        if (!(path instanceof S3Path))
            throw new IllegalArgumentException("path must be an S3 Path");
        S3Path s3Path = (S3Path) path;

        if (type.equals(BasicFileAttributeView.class) || type.equals(S3FileAttributeView.class)) {
            @SuppressWarnings("unchecked") final V v = (V) new S3FileAttributeView(s3Path);
            return v;
        } else {
            throw new IllegalArgumentException("type must be BasicFileAttributeView.class or S3FileAttributeView.class");
        }
    }

    /**
     * Reads a file's attributes as a bulk operation. This method works in
     * exactly the manner specified by the {@link
     * Files#readAttributes(Path, Class, LinkOption[])} method.
     *
     * @param path    the path to the file
     * @param type    the {@code Class} of the file attributes required
     *                to read. Supported types are {@code BasicFileAttributes} and {@code S3FileAttributes}
     * @param options options indicating how symbolic links are handled
     * @return the file attributes or {@code null} if {@code path} is inferred to be a directory.
     */
    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(type);
        if (!(path instanceof S3Path))
            throw new IllegalArgumentException("path must be an S3Path instance");
        S3Path s3Path = (S3Path) path;
        S3AsyncClient s3Client = s3Path.getFileSystem().client();

        if (type.equals(BasicFileAttributes.class) || type.equals(S3BasicFileAttributes.class)) {
            @SuppressWarnings("unchecked")
            A a = (A) new S3BasicFileAttributes(s3Path, s3Client);
            return a;
        } else {
            throw new UnsupportedOperationException("cannot read attributes of type: " + type);
        }
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path path = ...;
     *             S3AsyncClient s3 = ...;
     *
     *             p.readAttributes(s3, dir, type);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path path = ...;
     *
     *             p.readAttributes(path, type);
     *             </pre>
     */
    @Deprecated
    protected <A extends BasicFileAttributes> A readAttributes(S3AsyncClient s3AsyncClient, Path path, Class<A> type, LinkOption... options) {
        return readAttributes(forceAwsClient(path, s3AsyncClient), type, options);
    }

    /**
     * Reads a set of file attributes as a bulk operation. Largely equivalent to {@code readAttributes(Path path, Class<A> type, LinkOption... options)}
     * where the returned object is a map of method names (attributes) to values, filtered on the comma separated {@code attributes}.
     *
     * @param path       the path to the file
     * @param attributes the comma separated attributes to read. May be prefixed with "s3:"
     * @param options    ignored, S3 has no links
     * @return a map of the attributes returned; may be empty. The map's keys
     * are the attribute names, its values are the attribute values. Returns an empty map if {@code attributes} is empty,
     * or if {@code path} is inferred to be a directory.
     * @throws UnsupportedOperationException if the attribute view is not available
     * @throws IllegalArgumentException      if no attributes are specified or an unrecognized attributes is
     *                                       specified
     * @throws SecurityException             In the case of the default provider, and a security manager is
     *                                       installed, its {@link SecurityManager#checkRead(String) checkRead}
     *                                       method denies read access to the file. If this method is invoked
     *                                       to read security sensitive attributes then the security manager
     *                                       may be invoked to check for additional permissions.
     */
    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(attributes);
        S3Path s3Path = (S3Path) path;

        S3AsyncClient s3Client = s3Path.getFileSystem().client();

        if (s3Path.isDirectory() || attributes.trim().isEmpty())
            return Collections.emptyMap();

        if (attributes.equals("*") || attributes.equals("s3"))
            return new S3BasicFileAttributes(s3Path, s3Client).asMap();

        final Set<String> attrSet = Arrays.stream(attributes.split(","))
                .map(attr -> attr.replaceAll("^s3:", ""))
                .collect(Collectors.toSet());
        return readAttributes(path, S3BasicFileAttributes.class, options)
                .asMap(attrSet::contains);
    }

    /**
     *
     * @deprecated in favour of using a proper S3ClientProvider in S3FileSystem.
     *             For instance, instead of the following code:
     *             <pre>
     *             S3FileSystemProvider p = ...;
     *             S3Path path = ...;
     *             S3AsyncClient s3 = ...;
     *
     *             p.readAttributes(s3, dir, attributes);
     *             </pre>
     *             something equivalent to the below should be used:
     *             <pre>
     *             S3FileSystemProvider p = new MyFileSystemProvider(new MyClientProvider());
     *             S3Path path = ...;
     *
     *             p.readAttributes(path, attributes);
     *             </pre>
     */
    @Deprecated
    protected void readAttributes(S3AsyncClient client, Path path, String attributes, LinkOption... options) {
        readAttributes(forceAwsClient(path, client), attributes, options);
    }

    /**
     * File attributes of S3 objects cannot be set other than by creating a new object
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("s3 file attributes cannot be modified by this class");
    }

    private static List<List<ObjectIdentifier>> getContainedObjectBatches(S3AsyncClient s3Client, String bucketName, String prefix, long timeOut, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        String continuationToken = null;
        boolean hasMoreItems = true;
        List<List<ObjectIdentifier>> keys = new ArrayList<>();

        while (hasMoreItems) {
            String finalContinuationToken = continuationToken;
            ListObjectsV2Response response = s3Client.listObjectsV2(req -> req
                            .bucket(bucketName)
                            .prefix(prefix)
                            .continuationToken(finalContinuationToken))
                    .get(timeOut, unit);
            List<ObjectIdentifier> objects = response.contents()
                    .stream()
                    .filter(s3Object -> s3Object.key().equals(prefix) || s3Object.key().startsWith(prefix + S3Path.PATH_SEPARATOR))
                    .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                    .collect(Collectors.toList());
            if (!objects.isEmpty()) {
                keys.add(objects);
            }
            hasMoreItems = response.isTruncated();
            continuationToken = response.nextContinuationToken();
        }
        return keys;
    }

    private String getFileSystemKey(URI uri) {
        //
        // TODO: this duplicates the logic in S3FileSystem and assumes the key
        //       is the bucket name; we need to move the source of truth in
        //       S3FileSystemProvider or a separate class and pass it to the
        //       file system constructor.
        //
        return uri.getAuthority();
    }

    private S3Path forceAwsClient(final Path path, final S3AsyncClient client) {
        S3Path p = (S3Path)path;
        p.getFileSystem().clientProvider(new FixedS3ClientProvider(client));
        return p;
    }
}
