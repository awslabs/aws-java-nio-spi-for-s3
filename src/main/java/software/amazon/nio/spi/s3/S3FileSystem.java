/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import static software.amazon.nio.spi.s3.Constants.PATH_SEPARATOR;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channel;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.nio.spi.s3.config.S3NioSpiConfiguration;

/**
 * A Java NIO FileSystem for an S3 bucket as seen through the lens of the AWS Principal calling the class.
 */
public class S3FileSystem extends FileSystem {
    static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);

    /**
     * View required by Java NIO
     */
    static final String BASIC_FILE_ATTRIBUTE_VIEW = "basic";

    private static final Set<String> SUPPORTED_FILE_ATTRIBUTE_VIEWS =
        Collections.singleton(BASIC_FILE_ATTRIBUTE_VIEW);

    S3ClientProvider clientProvider;

    private final String bucketName;
    private final S3FileSystemProvider provider;
    private boolean open = true;
    private final Set<S3SeekableByteChannel> openChannels = new HashSet<>();

    // private S3AsyncClient client;
    private final S3NioSpiConfiguration configuration;
    private final Path temporaryDirectory;

    /**
     * Create a filesystem that represents the bucket specified by the URI
     *
     * @param provider the provider to be used with this fileSystem
     * @param config   the configuration to use; can be null to use a default configuration
     */
    S3FileSystem(S3FileSystemProvider provider, S3NioSpiConfiguration config) {
        this(provider, config, createTemporaryDirectory());
    }

    /**
     * Create a filesystem that represents the bucket specified by the URI
     *
     * @param provider
     *            the provider to be used with this fileSystem
     * @param config
     *            the configuration to use; can be null to use a default configuration
     * @param temporaryDirectory
     *            the local temporary directory for the S3 object
     */
    S3FileSystem(S3FileSystemProvider provider, S3NioSpiConfiguration config, Path temporaryDirectory) {
        configuration = (config == null) ? new S3NioSpiConfiguration() : config;
        bucketName = configuration.getBucketName();

        // This is quite questionable and may be removed in future versions:
        provider.setConfiguration(config);
        // The configuration field in {@code S3FileSystemProvider} is used for certain tasks
        // that are implemented there.
        // But those tasks are in service of {@code S3FileSystem} instances.
        // So if there are multiple ones with different configurations, the provider will use
        // the one that has been set by the last created filesystem, overriding potentially
        // different values in older {@code S3FileSystem} instances.
        //
        // See https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/597

        logger.debug("creating FileSystem for '{}://{}'", provider.getScheme(), bucketName);

        clientProvider = new S3ClientProvider(configuration);
        this.provider = provider;
        this.temporaryDirectory = temporaryDirectory;
    }

    static Path createTemporaryDirectory() {
        try {
            return Files.createTempDirectory("aws-s3-nio-");
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }

    /**
     * Returns the provider that created this file system.
     *
     * @return The provider that created this file system.
     */
    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    /**
     * Closes this file system.
     *
     * <p> After a file system is closed then all subsequent access to the file
     * system, either by methods defined by this class or on objects associated
     * with this file system, throw {@link ClosedFileSystemException}. If the
     * file system is already closed then invoking this method has no effect.
     *
     * <p> Closing a file system will close all open {@link
     * Channel channels}, {@link DirectoryStream directory-streams},
     * {@link WatchService watch-service}, and other closeable objects associated
     * with this file system. The {@link FileSystems#getDefault default} file
     * system cannot be closed.
     */
    @Override
    public void close() throws IOException {
        open = false;
        for (var channel : openChannels) {
            if (channel.isOpen()) {
                channel.close();
            }
            deregisterClosedChannel(channel);
        }
        provider.closeFileSystem(this);
    }

    /**
     * Returns the implementation for creating a checksum to check the integrity of an object uploaded to S3.
     *
     * @return integrity check implementation
     */
    S3ObjectIntegrityCheck integrityCheck() {
        var algorithm = configuration.getIntegrityCheckAlgorithm();
        if (algorithm.equalsIgnoreCase("CRC32")) {
            return new Crc32FileIntegrityCheck();
        }
        if (algorithm.equalsIgnoreCase("CRC32C")) {
            return new Crc32cFileIntegrityCheck();
        }
        if (algorithm.equalsIgnoreCase("CRC64NVME")) {
            return new Crc64nvmeFileIntegrityCheck();
        }
        return DisabledFileIntegrityCheck.INSTANCE;
    }

    /**
     * Tells whether this file system is open.
     *
     * <p> File systems created by the default provider are always open.
     *
     * @return {@code true} if, and only if, this file system is open
     */
    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Tells whether this file system allows only read-only access to
     * its file stores.
     * <br>
     * This is currently always false. The ability to write an individual object depend on the IAM role that is used by
     * the principal and the ACL of the bucket, but S3 itself is not inherently read only.
     *
     * @return {@code false}
     */
    @Override
    public boolean isReadOnly() {
        return false;
    }

    /**
     * Returns the name separator '/', represented as a string.
     *
     * <p> The name separator is used to separate names in a path string. An
     * implementation may support multiple name separators in which case this
     * method returns an implementation specific <em>default</em> name separator.
     * This separator is used when creating path strings by invoking the {@link
     * Path#toString() toString()} method.
     *
     * <p> In the case of the default provider, this method returns the same
     * separator as {@link File#separator}.
     *
     * @return The name separator "/"
     */
    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    /**
     * Returns an object to iterate over the paths of the root directories.
     *
     * <p> A file system provides access to a file store that may be composed
     * of a number of distinct file hierarchies, each with its own top-level
     * root directory. Unless denied by the security manager, each element in
     * the returned iterator corresponds to the root directory of a distinct
     * file hierarchy. The order of the elements is not defined. The file
     * hierarchies may change during the lifetime of the Java virtual machine.
     * For example, in some implementations, the insertion of removable media
     * may result in the creation of a new file hierarchy with its own
     * top-level directory.
     *
     * <p> When a security manager is installed, it is invoked to check access
     * to the root directory. If denied, the root directory is not returned
     * by the iterator. In the case of the default provider, the {@link
     * SecurityManager#checkRead(String)} method is invoked to check read access
     * to each root directory. It is system dependent if the permission checks
     * are done when the iterator is obtained or during iteration.
     *
     * @return An object to iterate over the root directories
     */
    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singleton(S3Path.getPath(this, "/"));
    }

    /**
     * An S3 bucket has no partitions, size limits or limits on the number of objects stored so there are no FileStores.
     *
     * @return An immutable empty set
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterable<FileStore> getFileStores() {
        return Collections.EMPTY_SET;
    }

    /**
     * Returns the set of the file attribute views supported by this {@code FileSystem}.
     * <br>
     * This FileSystem currently supports only the "basic" file attribute view.
     *
     * @return An unmodifiable set of the names of the supported file attribute
     * views
     */
    @Override
    public Set<String> supportedFileAttributeViews() {
        return SUPPORTED_FILE_ATTRIBUTE_VIEWS;
    }

    /**
     * Converts an S3 object path string, or a sequence of strings that when joined form
     * a path string, to a {@code S3Path}.
     *
     * <p>If {@code more} does not specify any
     * elements then the value of the {@code first} parameter is the path string
     * to convert. If {@code more} specifies one or more elements, then each
     * non-empty string, including {@code first}, is considered to be a sequence
     * of name elements (see {@link Path}) and is joined to form a path string.
     * The details as to how the Strings are joined is provider specific, but
     * typically they will be joined using the {@link #getSeparator
     * name-separator} as the separator. For example, if the name separator is
     * "{@code /}" and {@code getPath("/foo","bar","gus")} is invoked, then the
     * path string {@code "/foo/bar/gus"} is converted to a {@code Path}.
     * A {@code Path} representing an empty path is returned if {@code first}
     * is the empty string and {@code more} does not contain any non-empty
     * strings.
     *
     * <p> The parsing and conversion to a path object is inherently
     * implementation dependent. In the simplest case, the path string is rejected,
     * and {@link InvalidPathException} thrown, if the path string contains
     * characters that cannot be converted to characters that are <em>legal</em>
     * to the file store. For example, on UNIX systems, the NUL (&#92;u0000)
     * character is not allowed to be present in a path. An implementation may
     * choose to reject path strings that contain names that are longer than those
     * allowed by any file store, and where an implementation supports a complex
     * path syntax, it may choose to reject path strings that are <em>badly
     * formed</em>.
     *
     * <p> In the case of the default provider, path strings are parsed based
     * on the definition of paths at the platform or virtual file system level.
     * For example, an operating system may not allow specific characters to be
     * present in a file name, but a specific underlying file store may impose
     * different or additional restrictions on the set of legal
     * characters.
     *
     * <p> This method throws {@link InvalidPathException} when the path string
     * cannot be converted to a path. Where possible, and where applicable,
     * the exception is created with an {@link InvalidPathException#getIndex
     * index} value indicating the first position in the {@code path} parameter
     * that caused the path string to be rejected.
     *
     * @param first the path string or initial part of the path string
     * @param more  additional strings to be joined to form the path string
     * @return the resulting {@code Path}
     * @throws InvalidPathException If the path string cannot be converted
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public Path getPath(String first, String... more) {
        return S3Path.getPath(this, first, more);
    }

    /**
     * Returns a {@code PathMatcher} that performs match operations on the
     * {@code String} representation of {@link Path} objects by interpreting a
     * given pattern.
     * <p>
     * The {@code syntaxAndPattern} parameter identifies the syntax and the
     * pattern and takes the form:
     * <blockquote><pre>
     * <i>syntax</i><b>:</b><i>pattern</i>
     * </pre></blockquote>
     * where {@code ':'} stands for itself.
     *
     * <p> A {@code FileSystem} implementation supports the "{@code glob}" and
     * "{@code regex}" syntax's. The value of the syntax
     * component is compared without regard to case.
     *
     * <p> When the syntax is "{@code glob}" then the {@code String}
     * representation of the path is matched using a limited pattern language
     * that resembles regular expressions but with a simpler syntax.
     *
     * <p> The following rules are used to interpret glob patterns:
     *
     * <ul>
     *   <li><p> The {@code *} character matches zero or more {@link Character
     *   characters} of a {@link Path#getName(int) name} component without
     *   crossing directory boundaries. </p></li>
     *
     *   <li><p> The {@code **} characters matches zero or more {@link Character
     *   characters} crossing directory boundaries. </p></li>
     *
     *   <li><p> The {@code ?} character matches exactly one character of a
     *   name component.</p></li>
     *
     *   <li><p> The backslash character ({@code \}) is used to escape characters
     *   that would otherwise be interpreted as special characters. The expression
     *   {@code \\} matches a single backslash and "\{" matches a left brace
     *   for example.  </p></li>
     *
     *   <li><p> The {@code [ ]} characters are a <i>bracket expression</i> that
     *   match a single character of a name component out of a set of characters.
     *   For example, {@code [abc]} matches {@code "a"}, {@code "b"}, or {@code "c"}.
     *   The hyphen ({@code -}) may be used to specify a range so {@code [a-z]}
     *   specifies a range that matches from {@code "a"} to {@code "z"} (inclusive).
     *   These forms can be mixed so [abce-g] matches {@code "a"}, {@code "b"},
     *   {@code "c"}, {@code "e"}, {@code "f"} or {@code "g"}. If the character
     *   after the {@code [} is a {@code !} then it is used for negation so {@code
     *   [!a-c]} matches any character except {@code "a"}, {@code "b"}, or {@code
     *   "c"}.
     *   <p> Within a bracket expression the {@code *}, {@code ?} and {@code \}
     *   characters match themselves. The ({@code -}) character matches itself if
     *   it is the first character within the brackets, or the first character
     *   after the {@code !} if negating.</p></li>
     *
     *   <li><p> The {@code { }} characters are a group of sub-patterns, where
     *   the group matches if any subpattern in the group matches. The {@code ","}
     *   character is used to separate the sub-patterns. Groups cannot be nested.
     *   </p></li>
     *
     *   <li><p> Leading period dot characters in file name are
     *   treated as regular characters in match operations. For example,
     *   the {@code "*"} glob pattern matches file name {@code ".login"}.
     *   The {@link Files#isHidden} method may be used to test whether a file
     *   is considered hidden.
     *   </p></li>
     *
     *   <li><p> All other characters match themselves in an implementation
     *   dependent manner. This includes characters representing any {@link
     *   FileSystem#getSeparator name-separators}. </p></li>
     *
     *   <li><p> The matching of {@link Path#getRoot root} components is highly
     *   implementation-dependent and is not specified. </p></li>
     *
     * </ul>
     *
     * <p> When the syntax is "{@code regex}" then the pattern component is a
     * regular expression as defined by the {@link Pattern}
     * class.
     *
     * <p>  For both the glob and regex syntaxes, the matching details, such as
     * whether the matching is case-sensitive, are implementation-dependent
     * and therefore not specified.
     *
     * @param syntaxAndPattern The syntax and pattern
     * @return A path matcher that may be used to match paths against the pattern
     * @throws IllegalArgumentException      If the parameter does not take the form: {@code syntax:pattern}
     * @throws PatternSyntaxException        If the pattern is invalid
     * @throws UnsupportedOperationException If the pattern syntax is not known to the implementation
     * @see Files#newDirectoryStream(Path, String)
     */
    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        final int colonIndex = syntaxAndPattern.indexOf(':');
        if (colonIndex <= 0 || colonIndex == syntaxAndPattern.length() - 1) {
            throw new IllegalArgumentException("syntaxAndPattern must be of the form: syntax:pattern");
        }
        
        final String syntax = syntaxAndPattern.substring(0, colonIndex).toLowerCase();
        final String pattern = syntaxAndPattern.substring(colonIndex + 1);
        
        if ("strict-posix-glob".equals(syntax)) {
            // Use our strict POSIX glob implementation
            return new software.amazon.nio.spi.s3.util.StrictPosixGlobPathMatcher(pattern);
        } else {
            // Delegate to default implementation for other syntaxes
            return FileSystems.getDefault().getPathMatcher(syntaxAndPattern);
        }
    }

    /**
     * Currently Not Implemented
     *
     * @return The {@code UserPrincipalLookupService} for this file system
     * @throws UnsupportedOperationException If this {@code FileSystem} does not does have a lookup service
     */
    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException(
            "This method is not yet supported. Please raise a feature request describing your use case");
    }

    /**
     * Currently not implemented
     *
     * @return a new watch service
     * @throws UnsupportedOperationException If this {@code FileSystem} does not support watching file system
     *                                       objects for changes and events. This exception is not thrown
     *                                       by {@code FileSystems} created by the default provider.
     */
    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException(
            "This method is not yet supported. Please raise a feature request describing your use case");
    }

    /**
     * Returns the configuration object passed in the constructor or created
     * by default.
     *
     * @return the configuration object for this file system
     */
    public S3NioSpiConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Returns the configuration object passed in the constructor or created
     * by default.
     *
     * @return the configuration object for this file system
     *
     * @deprecated This method is deprecated and may be removed in a future release.
     *   Use {@link #getConfiguration()} instead to retrieve the configuration object,
     *   as it provides the same functionality with better naming conventions that
     *   align with standard practices.
     */
    @Deprecated
    public S3NioSpiConfiguration configuration() {
        return getConfiguration();
    }

    /**
     * Returns the client provider used to build aws clients
     *
     * @return the client provider
     */
    public S3ClientProvider clientProvider() {
        return clientProvider;
    }

    /**
     * Sets the client provider to use to build aws clients
     *
     * @param clientProvider the client provider
     */
    public void clientProvider(S3ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    /**
     * @return the S3Client associated with this FileSystem
     */
    S3AsyncClient client() {
        return clientProvider.generateClient(bucketName);
    }

    /**
     * Obtain the name of the bucket represented by this <code>FileSystem</code> instance
     *
     * @return the bucket name
     */
    String bucketName() {
        return bucketName;
    }

    /**
     * The list of currently open channels. Exposed mainly for testing
     *
     * @return a read only view wrapping the set of currently open channels.
     */
    Set<Channel> getOpenChannels() {
        return Collections.unmodifiableSet(openChannels);
    }

    void registerOpenChannel(S3SeekableByteChannel channel) {
        openChannels.add(channel);
    }

    boolean deregisterClosedChannel(S3SeekableByteChannel closedChannel) {
        assert !closedChannel.isOpen();

        return openChannels.remove(closedChannel);
    }

    /**
     * Creates a new local temporary file for an S3 object.
     *
     * @return new local temporary file
     */
    Path createTempFile(S3Path path) throws IOException {
        if (path.isDirectory()) {
            throw new IllegalArgumentException("path must be a file");
        }
        String filename = path.getFileName().toString();
        if (path.getNameCount() == 1) {
            Path newPath = temporaryDirectory.resolve(filename);
            newPath = Files.exists(newPath)
                ? temporaryDirectory.resolve(filename + "-" + System.nanoTime())
                : newPath;
            return Files.createFile(newPath);
        }
        Path parent = temporaryDirectory.resolve(path.getParent().toString());
        Files.createDirectories(parent);
        Path newPath = parent.resolve(filename);
        newPath = Files.exists(newPath)
            ? parent.resolve(filename + "-" + System.nanoTime())
            : newPath;
        return Files.createFile(newPath);
    }

    /**
     * Tests if two S3 filesystems are equal
     *
     * @param o the object to test for equality
     * @return true if {@code o} is not null, is an {@code S3FileSystem} and uses the same {@code S3FileSystemProvider} class
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (S3FileSystem) o;
        return bucketName.equals(that.bucketName) && provider.getClass().getName().equals(that.provider.getClass().getName());
    }

    @Override
    public int hashCode() {
        //CHECKSTYLE:OFF - There is no hashCode for multiple values
        return Objects.hash(bucketName, provider.getClass().getName());
        //CHECKSTYLE:ON
    }
}
