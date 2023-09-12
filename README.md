[![Java CI with Gradle](https://github.com/awslabs/aws-java-nio-spi-for-s3/actions/workflows/gradle.yml/badge.svg)](https://github.com/awslabs/aws-java-nio-spi-for-s3/actions/workflows/gradle.yml)

# AWS Java NIO SPI for S3

A Java NIO.2 service provider for S3, allowing Java NIO operations to be performed on paths using the `s3` scheme. This
package implements the service provider interface (SPI) defined for Java NIO.2 in JDK 1.7 providing "plug-in" non-blocking
access to S3 objects for Java applications using Java NIO.2 for file access. Using this package allows Java applications
to access S3 without having to modify or recompile the application. You also avoid having to set up any kind of FUSE mount.

For a general overview see the
[AWS Blog Post](https://aws.amazon.com/blogs/storage/extending-java-applications-to-directly-access-files-in-amazon-s3-without-recompiling/)
announcing this package.

## Using this package as a provider

There are several ways that this package can be used to provide Java NIO operations on S3 objects:

1. Use this libraries jar as one of your applications compile dependencies
2. Include the libraries "shadowJar" in your `$JAVA_HOME/jre/lib/ext/` directory (not supported for Java 9 and above)
3. Include this library on your class path at runtime  (best option for Java 9 and above)
4. Include the library as an extension at runtime `-Djava.ext.dirs=$JAVA_HOME/jre/lib/ext:/path/to/extension/` (not supported for Java 9 and above)

## Example usage

Assuming that `myExecutatbleJar` is a Java application that has been built to read from `java.nio.file.Path`s and
this library has been exposed by one of the mechanisms above then S3 URIs may be used to identify inputs. For example:

```
java -jar myExecutableJar --input s3://some-bucket/input/file
java -jar myExecutableJar --input s3x://my-s3-service:9000/some-bucket/input/file
```

If this library is exposed as an extension (see above), then no code changes or recompilation of `myExecutable` are
required.

### Using this package as a provider for Java 9 and above

With the introduction of modules in Java 9 the extension mechanism was retired. Providers should now be supplied as java modules.
For backward compatability we have not yet made this change so to ensure that the provider in this package is recognized
by the JVM you need to supply the JAR on your classpath using the `-classpath` flag. For example to use this provider with `org.example.myapp.Main` from `myApp.jar`
you would type the following:

```
java -classpath build/libs/nio-spi-for-s3-1.1.0-all.jar:myApp.jar org.example.myapp.Main
```

As a concrete example, using Java 9+ with the popular genomics application [GATK](https://gatk.broadinstitute.org/hc/en-us), you could do the following:

```
java -classpath build/libs/nio-spi-for-s3-1.1.0-all.jar:gatk-package-4.2.2.0-local.jar org.broadinstitute.hellbender.Main CountReads -I s3://<some-bucket>/ena/PRJEB3381/ERR194158/ERR194158.hg38.bam
```

## Including as a dependency

Releases of this library are available from Maven Central and can be added to projects using the standard dependency
declarations.

For example:

```xml
<dependency>
    <groupId>software.amazon.nio.s3</groupId>
    <artifactId>aws-java-nio-spi-for-s3</artifactId>
    <version>1.2.3</version>
</dependency>
```

## AWS Credentials

This library will perform all actions using credentials according to the AWS SDK for Java [default credential provider
chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html). The library does not allow any
library specific configuration of credentials. In essence, you (or the service / Principal
using this library) should have, or be able to assume, a role that will allow access to the S3 buckets and objects you
want to interact with.

Note, although your IAM role may be sufficient to access the desired objects and buckets you may still be
blocked by bucket access control lists and/ or bucket policies.

## S3 Compatible Endpoints and Credentials

This NIO provider supports any S3-like service. To access a 3rd party service,
follow this URI pattern:

```
s3x://[key:secret@]endpoint[:port]/bucket/objectkey
```
Note that in this case the TCP port of the target service must be specified.
If no credentials are given the default AWS configuration mechanism will be used as per
the section above.

In the case the target service uses HTTP instead of HTTPS (e.g. a testing environment),
the protocol to use can be fonfigured through the following environment variable or system
property:

```
export S3_SPI_ENDPOINT_PROTOCOL=http
java -Ds3.spi.endpoint-protocol=http
```

The same can also be provided when creating a file system:

```
Map<String, String> env = ...;
env.put("s3.spi.endpoint-protocol", "http");
FileSystem fs = FileSystems.newFileSystem("s3://myendpoint.com:1000/mybucket", env);
```

## Reading Files

Bytes from S3 objects can be read using an `S3SeekableByteChannel` which is an implementation of `java.nio.channel.SeekableByteChannel`.
Because S3 is a high-throughput but high-latency (compared to a native filesystem) service the `S3SeekableByteChannel`
uses an in-memory read-ahead cache of `ByteBuffers` and is optimized for the scenario where bytes will typically be
read sequentially.

To perform this the `S3SeekableByteChannel` delegates read operations to an `S3ReadAheadByteChannel` which
implements `java.nio.channels.ReadableByteChannel`. When the first `read` operation is called, the channel will read it's
first fragment and enter that into the buffer, requests for bytes in that fragment are fulfilled from that buffer. When
a buffer fragment is more than half read, all empty fragment slots in the cache will be asynchronously filled. Further,
any cached fragments that precede the fragment currently being read will be invalidated in the cache freeing up space
for additional fragments to be retrieved asynchronously. Once the cache is "warm" the application should not be blocked
on I/O, up to the limits of your network connection.

### Configuration

The read-ahead buffer asynchronously prefetches `n` sequential fragments of `m` bytes from S3. The
values of `n` and `m` can be configured to your needs by using command line properties or environment variables.

If no configuration is supplied the values in `resources/s3-nio-spi.properties` are used. Currently, 50 fragments of 5MB.
Each fragment is downloaded concurrently on a unique thread.

#### Environment Variables

You may use `S3_SPI_READ_MAX_FRAGMENT_NUMBER` and `S3_SPI_READ_MAX_FRAGMENT_SIZE` to set the maximum umber of cached
fragments and maximum fragment sizes respectively. For example:

```shell
export S3_SPI_READ_MAX_FRAGMENT_SIZE=100000
export S3_SPI_READ_MAX_FRAGMENT_NUMBER=5
java -Djava.ext.dirs=$JAVA_HOME/jre/lib/ext:<location-of-this-spi-jar> -jar <jar-file-to-run>
```

#### Java Properties

You may use java command line properties to set the values of the maximum fragment size and maximum number of fragments
with `s3.spi.read.max-fragment-size` and `s3.spi.read.max-fragment-number` respectively. For example:

```shell
java -Djava.ext.dirs=$JAVA_HOME/jre/lib/ext:<location-of-this-spi-jar> -Ds3.spi.read.max-fragment-size=10000 -Ds3.spi.read.max-fragment-number=2 -jar <jar-file-to-run>
```

#### Order of Precedence

Configurations use the following order of precedence from highest to lowest:

1. Java properties
2. Environment variables
3. Default values

#### S3 limits

As each `S3SeekableByteChannel` can potentially spawn 50 concurrent fragment download threads, you may find you exceed S3
limits, especially when the application using this SPI reads from multiple files at the same time or has multiple threads
each opening its own byte channel. In this situation you should reduce the size of `S3_SPI_READ_MAX_FRAGMENT_NUMBER`.

In some cases it may also help to increase the value of `S3_SPI_READ_MAX_FRAGMENT_SIZE` as fewer, large fragments will
reduce the number of requests to the S3 service.

Ensure sufficient memory is available to your JVM if you increase the fragment size or fragment number.

## Writing Files

The mode of the channel is controlled with the `StandardOpenOptions`. To open a channel for write access you need to
supply the option `StandardOpenOption.WRITE`. All write operations on the channel will be gathered in a temporary file,
which will be uploaded to S3 upon closing the channel.

Be aware, that the current implementation only supports channels to be used either for read or write due to potential
consistency issues we may face in some cases. Attempting to open a channel for both read and write will result in an error.

### Configuration

Because we cannot predict the time it would take to write files, there are currently no timeouts configured per
default. However, you may configure timeouts via the `S3SeekableByteChannel`.

#### Timeouts
To configure timeouts for writing files or opening files for write access, you may use the `Long timeout` and
`TimeUnit timeUnit` parameters of the `S3SeekableByteChannel` constructor.

```
new S3SeekableByteChannel(s3Path, s3Client, channelOpenOptions, timeout, timeUnit);
```

## Design Decisions

As an object store, S3 is not completely analogous to a traditional file system. Therefore, several opinionated decisions
were made to map filesystem concepts to S3 concepts.

### A Bucket is a `FileSystem`

An S3 bucket is represented as a `java.nio.spi.FileSystem` using an `S3FileSystem`. Although buckets are globally
namespaced they are owned by individual accounts, have their own permissions, regions, and potentially, endpoints.
An application that accesses objects from multiple buckets will generate multiple `FileSystem` instances.

### S3 Objects are `Path`s

Objects in S3 are analogous to files in a filesystem and are identified using `S3Path` instances which can be built
using S3 uris (e.g `s3://mybucket/some-object`) or, posix patterns `/some-object` from an `S3FileSystem` for `mybucket`

### No hidden files

S3 doesn't support hidden files therefore objects in S3 named with a `.` prefix such as `.hidden` are not considered hidden
by this library.

### Creation time and Last modified time

Creation time and Last modified time are always identical. S3 objects do not have a creation time, and modification of
an S3 object is actually a re-write of the object so these
are both given the same date (represented as a `FileTime`). If for some reason a last modified time cannot be determined
the Unix Epoch zero-time is used.

### No symbolic links

S3 doesn't support symbolic links therefore no `S3Path` is a symbolic link and any NIO `LinkOption`s are ignored when resolving
`Path`s.

### Posix-like path representations

Technically, S3 doesn't have directories - there are only buckets and keys. For example, in `s3://mybucket/path/to/file/object`
the bucket name is `mybucket` and the key would be `/path/to/file/object`. By convention, the use of `/` in a key is
thought of as a path separator. Therefore, `object` could be inferred to be a file in a directory called `/path/to/file/`
even though that directory technically doesn't exist. This package will infer directories under what we call "posix-like"
path representations. The logic of these is encoded in the `PosixLikePathRepresentation` object and described below.

#### Directories

An `S3Path` is inferred to be a directory if the path ends with `/`, `/.` or `/..` or contains only `.` or `..`.

For example, these paths are inferred to be directories `/dir/`, `/dir/.`, `/dir/..`. However `dir` and `/dir` cannot be inferred to be a directory.
This is a divergence from a true POSIX filesystem where if `/dir/` is a directory then `/dir` and `dir` relative to `/` must also be a directory.
S3 holds no metadata that can be used to make this inference.

#### Working directory

As directories don't exist and are only inferred there is no concept of being "in a directory". Therefore, the working
directory is always the root and `/object` `./object` and `object` can be inferred to be the same file. In addition `../object`
will also be the same file as you may not navigate above the root and no error will be produced if you attempt to.

#### Relative path resolution

Although there are no working directories, paths may be resolved relative to one another as long as one is a directory.
So if `some/path` was resolved relative to `/this/location/` then the resulting path is `/this/location/some/path`.

Because directories are inferred, you may not resolve `some/path` relative to `/this/location` as the latter cannot be
inferred to be a directory (it lacks a trailing `/`).

#### Resolution of `..` and `.`

The POSIX path special symbols `.` and `..` are treated as they would be in a normal POSIX path. Note that this could
cause some S3 objects to be effectively invisible to this implementation. For example `s3://mybucket/foo/./baa` is
an allowed S3 URI that is *not* equivalent to `s3://mybucket/foo/baa` even though this library will resolve the path `/foo/./baa`
to `/foo/baa`.

## Building this library

The library uses the gradle build system and targets Java 1.8 to allow it to be used in many contexts. To build you can simply run:

```shell
./gradlew build
```

This will run all unit tests and then generate a jar file in `libs` with the name `s3fs-spi-<version>.jar`. Note that
although the compiled JAR targets Java 1.8 a later version of the JDK may be needed to run Gradle itself.

### Shadowed Jar with dependencies

To build a "fat" jar with the required dependencies (including aws s3 client libraries) you can run:

```shell
./gradlew shadowJar
```

which will produce `s3fs-spi-<version>-all.jar`. If you are using this library as an extension, this is the recommended
jar to use. Don't put both jars on your classpath or extension path, you will observe class conflicts.

## Testing

To run unit tests and produce code coverage reports, run this command:

```shell
./gradlew test
```

HTML output of the test reports can be found at `build/reports/tests/test/index.html` and test coverage reports are
found at `build/reports/jacoco/test/html/index.html`

## Contributing

We encourage community contributions via pull requests. Please refer to our [code of conduct](./CODE_OF_CONDUCT.md) and
[contributing](./CONTRIBUTING.md) for guidance.

Code must compile to JDK 1.8 compatible bytecode. Matching unit tests are required for new features and fixes.

### Contributing Unit Tests

We use JUnit 5 and Mockito for unit testing.

When contributing code for bug fixes or feature improvements, matching tests should also be provided. Tests must not
rely on specific S3 bucket access or credentials. To this end, S3 clients and other artifacts should be mocked as
necessary. Remember, you are testing this library, not the behavior of S3.
