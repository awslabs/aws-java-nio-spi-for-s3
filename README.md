[![Java CI with Gradle](https://github.com/awslabs/aws-java-nio-spi-for-s3/actions/workflows/gradle.yml/badge.svg)](https://github.com/awslabs/aws-java-nio-spi-for-s3/actions/workflows/gradle.yml)
[![Github All Releases](https://img.shields.io/github/downloads/awslabs/aws-java-nio-spi-for-s3/total.svg)]()
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

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

Assuming that `myExecutableJar` is a Java application that has been built to read from `java.nio.file.Path`s and
this library has been exposed by one of the mechanisms above then S3 URIs may be used to identify inputs. For example:

```
java -jar myExecutableJar --input s3://some-bucket/input/file
java -jar myExecutableJar --input s3x://my-s3-service:9000/some-bucket/input/file
```

If this library is exposed as an extension (see above), then no code changes or recompilation of `myExecutable` are
required.

Several examples are included in the `examples` module. In most cases it is sufficient to use Java `Path` objects constructed
with a `URI` representing `s3://` URI. Constructing `Path`s with `String`s (as opposed to `URI`s) will normally default
to the local filesystems NIO provider rather than this provider so use of `URI`s should always be preferred over `String`s
for all `Path` objects (even local ones).

### Using this package as a provider for Java 9 and above

With the introduction of modules in Java 9 the extension mechanism was retired. Providers should now be supplied as java modules.
For backward compatibility we have not yet made this change so to ensure that the provider in this package is recognized
by the JVM you need to supply the JAR on your classpath using the `-classpath` flag. For example to use this provider with `org.example.myapp.Main` from `myApp.jar`
you would type the following:

```
java -classpath build/libs/nio-spi-for-s3-<version>-all.jar:myApp.jar org.example.myapp.Main
```

As a concrete example, using Java 9+ with the popular genomics application [GATK](https://gatk.broadinstitute.org/hc/en-us), you could do the following:

```
java -classpath build/libs/nio-spi-for-s3-<version>-all.jar:gatk-package-4.2.2.0-local.jar org.broadinstitute.hellbender.Main CountReads -I s3://<some-bucket>/ena/PRJEB3381/ERR194158/ERR194158.hg38.bam
```

## Including as a dependency

Releases of this library are available from Maven Central and can be added to projects using the standard dependency
declarations.

For example:

`build.pom`
```xml
<dependency>
    <groupId>software.amazon.nio.s3</groupId>
    <artifactId>aws-java-nio-spi-for-s3</artifactId>
    <version>2.5.0</version>
</dependency>
```

`build.gradle(.kts)`
```groovy
    implementation("software.amazon.nio.s3:aws-java-nio-spi-for-s3:2.5.0")
```

> [!TIP]
> If your application only uses standard Java NIO APIs and doesn't directly reference S3-specific classes, 
> you can use `runtimeOnly` instead of `implementation`. This allows the S3 provider to be discovered 
> automatically via Java's ServiceLoader mechanism:
> ```groovy
> runtimeOnly("software.amazon.nio.s3:aws-java-nio-spi-for-s3:2.5.0")
> ```
> This approach is cleaner as it keeps the S3 provider as a pure runtime dependency that's loaded dynamically.
>
> You can also use `latest.release` to automatically get the most recent stable version:
> ```groovy
> runtimeOnly("software.amazon.nio.s3:aws-java-nio-spi-for-s3:latest.release")
> ```
> **Note**: Using `latest.release` can lead to unexpected behavior if new versions introduce breaking changes. 
> Specifying exact versions is recommended for reproducible builds.

The library heavily relies on the `crt` client from aws. It uses the [`uber`
version](https://github.com/awslabs/aws-crt-java?tab=readme-ov-file#platform-specific-jars) for simplicity
and wide range of supported platforms. 

> [!TIP]
> If **size** is an **issue**, you can **exclude** the `crt` dependency from the library and import the [specific `crt` library](https://github.com/awslabs/aws-crt-java?tab=readme-ov-file#platform-specific-jars)
> for your platform. For example:
> ```
> implementation("software.amazon.nio.s3:aws-java-nio-spi-for-s3:2.5.0") {
>	exclude group: 'software.amazon.awssdk.crt', module: 'aws-crt'
> }
> implementation 'software.amazon.awssdk.crt:aws-crt:0.31.1:linux-x86_64'
> ```

### Java compatibility
| Library version | Java   |
|-----------------|--------|
| <= 1.2.1        | \>=  8 |
| \>= 2.x.x       | \>= 11 |

Versions 1.2.2 until 2.x.x are compatible with java 8 and above,
but will need to exclude logback dependency if using with java 8:
```groovy
implementation("software.amazon.nio.s3:aws-java-nio-spi-for-s3:1.2.2") {
    exclude group: 'ch.qos.logback', module: 'logback-core'
    exclude group: 'ch.qos.logback', module: 'logback-classic'
}
implementation("ch.qos.logback:logback-classic:1.3.11") // 1.3.x is still compatible with java 8
implementation("ch.qos.logback:logback-core:1.3.11")
```

## Client Identification Headers

This library can add custom headers to S3 requests to help identify traffic originating from this library. This is useful for:
- Monitoring and analytics in S3 access logs
- Identifying requests in AWS CloudTrail
- Debugging and troubleshooting

When enabled, the following headers are added to all S3 requests:
- `User-Agent: aws-java-nio-spi-for-s3/[version]`
- `X-Amz-Client-Name: aws-java-nio-spi-for-s3`
- `X-Amz-Client-Version: [version]`

### Enabling Custom Headers

**Method 1: System Property (Global)**
```bash
# Enable for all S3FileSystemProvider instances
java -Ds3.spi.client.custom-headers.enabled=true -jar your-application.jar
```

**Method 2: Programmatic (Per FileSystem)**
```java
S3FileSystemProvider provider = new S3FileSystemProvider();
// getPath materializes the file-system view on demand; getFileSystem(uri) throws
// FileSystemNotFoundException if the file system has not been created yet.
S3FileSystem fileSystem = (S3FileSystem) provider.getPath(URI.create("s3://your-bucket")).getFileSystem();
fileSystem.clientProvider().setCustomHeadersEnabled(true);
```

**Note:** Enabling custom headers switches from the high-performance CRT client to the regular AWS SDK client. For most use cases, the performance difference is negligible, but consider this for high-throughput applications.

## AWS Credentials

By default this library performs all actions using credentials according to the AWS SDK for Java [default credential provider
chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html). In essence, you (or the
service / Principal using this library) should have, or be able to assume, a role that will allow access to the S3 buckets
and objects you want to interact with.

You may also supply credentials explicitly per file system, either through the `env` map of
`FileSystems.newFileSystem(URI, env)` — as an `AwsCredentials` object under `s3.spi.credentials` or an
`AwsCredentialsProvider` under `s3.spi.credentials.provider` (see
[Per-filesystem configuration](#per-filesystem-configuration-via-newfilesystem)) — or via the
`s3x://key:secret@endpoint/...` URI form described below. When none are supplied, the default provider chain is used.

Note, although your IAM role may be sufficient to access the desired objects and buckets you may still be
blocked by bucket access control lists and/ or bucket policies.

## S3 Compatible Endpoints and Credentials

This NIO provider also supports 3rd party S3-like services. To access a 3rd party service,
follow this URI pattern:

```
s3x://[key:secret@]endpoint[:port]/bucket/objectkey
```

If no credentials are given the default AWS configuration mechanism will be used as per
the section above.

In the case the target service uses HTTP instead of HTTPS (e.g. a testing environment),
the protocol to use can be configured through the following environment variable or system
property:

```
export S3_SPI_ENDPOINT_PROTOCOL=http
java -Ds3.spi.endpoint-protocol=http
```

## Amazon S3 Access Points

[Access points](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points.html) are named network endpoints that are attached to buckets that you can use to perform S3 object operations.
To perform an operation via an access point using this library you will need to use the [access point alias](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points-alias.html)
as the access point arn is not a valid URI and cannot be used to form a Java `Path`.

### Limitations
- Not all S3 operations are [supported when using access links](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points-alias.html). If you notice a feature of this library that cannot be used via an access point please file an issue with this repository explaining your use case.
- Your actions may be additionally limited by policies present on the access point that are not present on the bucket

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

Configuration parameters can be supplied per file system (via the `env` map of
`FileSystems.newFileSystem(URI, env)` — see [Per-filesystem configuration](#per-filesystem-configuration-via-newfilesystem)
below) or globally as environment variables and Java system properties. Global values apply to all
file systems created with the S3 and S3X providers unless overridden per file system.

If no configuration is supplied, built-in defaults are used (for reads, 50 fragments of 5&nbsp;MB, each
downloaded concurrently on its own thread).

#### Supported keys

Values may be set programmatically (via `S3NioSpiConfiguration` `withXxx(...)` setters or the
`newFileSystem` env map) or as a system property / environment variable. The environment-variable
name is the system-property name uppercased with `.` and `-` replaced by `_` (e.g.
`s3.spi.read.max-fragment-size` → `S3_SPI_READ_MAX_FRAGMENT_SIZE`).

| system property | env var | description |
|---|---|---|
| **aws.region** | AWS_REGION | region for API calls; if unset, the AWS SDK default region provider chain resolves it |
| **s3.spi.endpoint** | S3_SPI_ENDPOINT | non-default `host[:port]` endpoint (mainly for S3-compatible services) |
| **s3.spi.endpoint-protocol** | S3_SPI_ENDPOINT_PROTOCOL | `http` or `https` (default `https`) |
| **s3.spi.force-path-style** | S3_SPI_FORCE_PATH_STYLE | force path-style addressing (default `false`) |
| **s3.spi.read.max-fragment-number** | S3_SPI_READ_MAX_FRAGMENT_NUMBER | number of sequential fragments prefetched (default 50) |
| **s3.spi.read.max-fragment-size** | S3_SPI_READ_MAX_FRAGMENT_SIZE | size of each fragment in bytes (default 5&nbsp;MB) |
| **s3.spi.timeout-low** / **-medium** / **-high** | S3_SPI_TIMEOUT_LOW / _MEDIUM / _HIGH | API timeouts in minutes |
| **s3.integrity-check-algorithm** | S3_INTEGRITY_CHECK_ALGORITHM | `disabled` (default), `CRC32`, `CRC32C`, or `CRC64NVME` |
| **s3.spi.write.streaming-multipart-upload** | S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD | enable streaming multipart upload (default `false`) |
| **s3.spi.write.multipart-part-size** | S3_SPI_WRITE_MULTIPART_PART_SIZE | multipart part size in bytes (default 8&nbsp;MB; 5&nbsp;MB–5&nbsp;GB) |
| **s3.spi.write.multipart-fallback-enabled** | S3_SPI_WRITE_MULTIPART_FALLBACK_ENABLED | enable temp-file fallback for streaming uploads (default `false`) |

Credentials are handled by the AWS SDK [default credential provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html)
and are **not** read from `aws.accessKeyId` / `aws.secretAccessKey` by this library. You may instead
supply credentials per file system (see below) either as an `AwsCredentials` object under
`s3.spi.credentials`, an `AwsCredentialsProvider` under `s3.spi.credentials.provider`, or via the
`s3x://key:secret@endpoint/...` URI form.

#### Environment Variables

For example, to set the read fragment size and number:

```shell
export S3_SPI_READ_MAX_FRAGMENT_SIZE=100000
export S3_SPI_READ_MAX_FRAGMENT_NUMBER=5
java -classpath <location-of-this-spi-jar> -jar <jar-file-to-run>
```

#### Java Properties

The same values can be set as Java system properties:

```shell
java -classpath <location-of-this-spi-jar> -Ds3.spi.read.max-fragment-size=10000 -Ds3.spi.read.max-fragment-number=2 -jar <jar-file-to-run>
```

#### Per-filesystem configuration via `newFileSystem`

Configuration can be supplied for a single file system through the `env` map of
`FileSystems.newFileSystem(URI, env)`. The map is merged into that file system's configuration at the
highest precedence (it overrides values parsed from the URI as well as system properties and
environment variables). Keys are the property names from the table above; values may be `String`s or
already-typed objects (e.g. an `AwsCredentials` / `AwsCredentialsProvider`). Bucket-creation keys
(`acl`, `grantFullControl`, `grantRead`, `grantReadACP`, `grantWrite`, `grantWriteACP`,
`locationConstraint`) continue to configure the underlying `createBucket` call and are not merged
into the configuration.

```java
Map<String, Object> env = new HashMap<>();
env.put(S3NioSpiConfiguration.AWS_REGION_PROPERTY, "eu-central-1");
env.put(S3NioSpiConfiguration.S3_SPI_TIMEOUT_LOW_PROPERTY, "2");
env.put(S3NioSpiConfiguration.S3_SPI_CREDENTIALS_PROPERTY,
        AwsBasicCredentials.create(accessKey, secretKey));

try (FileSystem fs = FileSystems.newFileSystem(URI.create("s3://my-bucket"), env)) {
    // ... use fs; ((S3FileSystem) fs).getConfiguration().getRegion() == "eu-central-1"
}
```

A file system's configuration is bound when the file system is first created (by `newFileSystem` or,
for `Paths.get(URI)` / `provider.getPath(URI)`, when the view is first materialized). Per the
`java.nio.file` contract, calling `newFileSystem` for a bucket that already has a file system in this
JVM throws `FileSystemAlreadyExistsException`. See the runnable
`ConfiguredFileSystemExample` in the `examples` module.

#### Order of Precedence

Configurations use the following order of precedence from highest to lowest:

1. Programmatic values — `S3NioSpiConfiguration` `withXxx(...)` setters, the `newFileSystem` env map, and values parsed from the URI
2. Java system properties
3. Environment variables
4. Default values

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

## Streaming Multipart Upload

Streaming multipart upload allows parts to be uploaded to S3 as data is written, rather than buffering everything
to a local temp file. This reduces memory and disk usage and provides incremental upload progress for large objects.

### Requirements

This feature requires the AWS CRT (Common Runtime) client. If the CRT client is not in use, an
`UnsupportedOperationException` is thrown when attempting to open a channel with the streaming multipart upload option.

### When to Use

Use streaming multipart upload when writing large objects (greater than 8 MiB) sequentially and you want to minimize
local disk and memory usage. This is not suitable for random-access write patterns — if a backward seek is detected,
the channel automatically falls back to the traditional temp-file approach.

### Configuration Options

| Option | Default | Min | Max | Description |
|--------|---------|-----|-----|-------------|
| Part size | 8 MiB | 5 MiB | 5 GiB | Size of each part uploaded to S3 |
| Max in-flight uploads | 4 | — | — | Maximum concurrent part uploads |

Memory usage is approximately `(maxInFlight + 1) × partSize`. With defaults this is about 40 MiB.

### Code Example

```java
// Open a channel with streaming multipart upload (default 8 MiB parts)
Set<OpenOption> options = Set.of(
    StandardOpenOption.WRITE,
    StandardOpenOption.CREATE,
    S3OpenOption.streamingMultipartUpload()
);
Path s3Path = Paths.get(URI.create("s3://my-bucket/large-file.dat"));
try (SeekableByteChannel channel = Files.newByteChannel(s3Path, options)) {
    ByteBuffer buffer = ByteBuffer.allocate(64 * 1024); // 64 KiB write buffer
    while (hasMoreData()) {
        fillBuffer(buffer);
        buffer.flip();
        channel.write(buffer);
        buffer.clear();
    }
}

// With custom part size (16 MiB)
Set<OpenOption> options = Set.of(
    StandardOpenOption.WRITE,
    StandardOpenOption.CREATE,
    S3OpenOption.streamingMultipartUpload(16 * 1024 * 1024)
);
```

### Environment Variable / System Property Configuration

SPI users who cannot pass custom open options programmatically can enable streaming multipart upload via
environment variables or system properties:

```bash
export S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD=true
export S3_SPI_WRITE_MULTIPART_PART_SIZE=16777216  # 16 MiB
export S3_SPI_WRITE_MULTIPART_FALLBACK_ENABLED=true  # enable fallback to temp-file on seeks
```

Or via system properties:

```bash
java -Ds3.spi.write.streaming-multipart-upload=true \
     -Ds3.spi.write.multipart-part-size=16777216 \
     -Ds3.spi.write.multipart-fallback-enabled=true \
     -jar my-application.jar
```

### Fallback Behavior

If any explicit position change is detected (either a backward seek or a forward seek that creates a gap), the
channel automatically aborts the multipart upload and falls back to the traditional temp-file approach. A warning
is logged when this occurs. Subsequent writes and the final upload on close behave identically to the standard
write channel.

This includes:
- **Backward seeks** — setting position to a value less than the current write position
- **Forward seeks (gaps)** — setting position to a value greater than the current write position, which would
  create a zero-filled hole in the data

On a standard filesystem, a forward seek past the end of written data creates a sparse file with implicit zeros
in the gap. Since streaming multipart upload cannot represent these gaps without materializing potentially large
amounts of zero bytes through the upload pipeline, the channel falls back to temp-file mode where the filesystem
handles sparse positioning natively.

Only purely sequential writes (where position advances naturally via `write()`) remain in streaming mode. Calling
`position(currentPosition)` (a no-op) does not trigger fallback.

#### Enabling Fallback

By default, the channel operates in strict append-only mode: seeks throw `UnsupportedOperationException` and
part data is not retained in memory after upload. This keeps memory usage bounded to approximately
`(maxInFlight + 1) × partSize`.

If your use case may involve non-sequential writes (e.g., seeking backward to overwrite a header), you can
enable fallback to temp-file mode:

```java
// Enable fallback: seeks trigger temp-file mode instead of throwing
// Tradeoff: all written data is retained in memory to support reconstruction
S3OpenOption.streamingMultipartUpload(8 * 1024 * 1024, true)
```

Or via environment variable / system property:
```bash
export S3_SPI_WRITE_MULTIPART_FALLBACK_ENABLED=true
```

When fallback is enabled:
- Non-sequential position changes trigger automatic fallback to temp-file mode
- All part data is retained in memory (total memory usage equals total bytes written)
- After fallback, the channel behaves identically to the standard write channel

### S3 Part Limits

S3 imposes hard limits on multipart uploads:

| Limit | Value |
|-------|-------|
| Maximum parts per upload | 10,000 |
| Minimum part size | 5 MiB (except the final part) |
| Maximum part size | 5 GiB |

The maximum uploadable object size is `partSize × 10,000`:

| Part Size | Max Object Size |
|-----------|-----------------|
| 8 MiB (default) | ~78 GiB |
| 100 MiB | ~976 GiB |
| 5 GiB | ~48.8 TiB (S3 limit is 5 TiB) |

If the part limit is exceeded during a write, the channel aborts the multipart upload and throws an
`IllegalStateException`. To upload larger objects, configure a larger part size.

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

For example, these paths are inferred to be directories `/dir/`, `/dir/.`, `/dir/..`. However `dir` and `/dir` cannot 
be inferred to be a directory.
This is a divergence from a true POSIX filesystem where if `/dir/` is a directory then `/dir` and `dir` relative 
to `/` must also be a directory. S3 holds no metadata that can be used to make this inference.

#### Working directory

As directories don't exist and are only inferred there is no concept of being "in a directory". The working directory
is always the root, so a relative path such as `object` refers to the same S3 object as the absolute path `/object`
once it is resolved against the root. `../object` also refers to that object because you may not navigate above the
root, and no error is produced if you attempt to.

> **Behavior change in 3.0:** `S3Path.equals`, `hashCode` and `compareTo` now compare the *abstract* path, matching the
> `java.nio.file.Path` contract. This means a relative path is **not** `equals()` to the corresponding absolute path
> (e.g. `object` is no longer `equals()` to `/object`), and `.`/`..` are not eliminated during comparison. Use
> `Files.isSameFile(a, b)` (or compare `toRealPath()`) to test whether two paths refer to the same object. See
> [Breaking changes in 3.0](#breaking-changes-in-30).

#### Relative path resolution

Although there are no working directories, paths may be resolved relative to one another as long as one is a directory.
So if `some/path` was resolved relative to `/this/location/` then the resulting path is `/this/location/some/path`.

Because directories are inferred, you may not resolve `some/path` relative to `/this/location` as the latter cannot be
inferred to be a directory (it lacks a trailing `/`).

The parent of an absolute path is always absolute (e.g. the parent of `/a/b/c` is `/a/b`), and `getParent()` returns
`null` once the root is reached, so walking a path to its root terminates cleanly.

#### Resolution of `..` and `.`

The POSIX path special symbols `.` and `..` are treated as they would be in a normal POSIX path. `normalize()` follows
the `java.nio.file.Path` contract: a leading `..` in a *relative* path is preserved (its target is unknown), while a
`..` at the root of an *absolute* path is dropped without making the path relative. Note that this could cause some S3
objects to be effectively invisible to this implementation. For example `s3://mybucket/foo/./baa` is an allowed S3 URI
that is *not* equivalent to `s3://mybucket/foo/baa` even though `normalize()` will reduce the path `/foo/./baa` to
`/foo/baa`.

### S3 "URI" and Java `URI` incompatibility

The definition of an S3 URI doesn't completely conform to the W3C specification. For example an object in `mybucket` called
`my%object` will result in an S3 URI called `s3://mybucket/my%object` even though the `%` symbol should be URL encoded.
The Java NIO libraries depend on the use of Java `URI` objects which are `final` and which *do* follow the W3C specification 
and therefore must URL encode the URI. This results in a small incompatibility where the above URI cannot be represented
by this library. Whenever possible avoiding the use of special characters in S3 filenames and paths is recommended. Otherwise
cautious use of URL escapes will be needed.

### `delete` follows the `Files.delete` contract

As of 3.0, `delete(path)` throws `NoSuchFileException` when the target does not exist and `DirectoryNotEmptyException`
when the path is a non-empty directory, matching `java.nio.file.Files.delete`. Each `delete` therefore performs a small
existence/emptiness check (a `HEAD` or a single-key `LIST`) before issuing the delete. To remove a directory together
with all of its contents, either move it (see below) or walk it bottom-up (for example with a
`Files.walkFileTree` deletion visitor).

### Copies (and moves) of a directory will also copy contents

Our implementation of `FileSystemProvider.copy` will also copy the content of the directory via batched copy operations. This is a variance
from some other implementations such as `UnixFileSystemProvider` where directory contents are not copied and the
use of the `walkFileTree` method is suggested to perform deep copies. In S3 this could result in an explosion
of API calls which would be both expensive in time and possibly money. By performing batch copies we can greatly reduce
the number of calls. `move` is implemented as a recursive copy followed by removal of the whole source subtree, and so
shares this behavior. Because S3 has no atomic rename, `move` with `StandardCopyOption.ATOMIC_MOVE` throws
`AtomicMoveNotSupportedException`.

## Breaking changes in 3.0

Release 3.0 tightens the provider's conformance to the `java.nio.file` contracts. The most visible changes are:

- **`Path` equality is abstract.** `S3Path.equals`, `hashCode` and `compareTo` compare the abstract path (and bucket),
  no longer normalizing via `toRealPath()`. A relative path is no longer `equals()` to the corresponding absolute path,
  and `.`/`..` are not eliminated during comparison. Use `Files.isSameFile` to test whether two paths locate the same
  object.
- **`getParent()` stays absolute** and returns `null` at the root (previously it could return a relative path and then
  throw while walking to the root).
- **`normalize()`** preserves a leading `..` in relative paths and keeps absolute paths absolute.
- **`relativize()`** now returns a correct, non-null relative path in all cases (including when `..` segments are
  required).
- **`iterator()`** no longer yields the root component as the first element.
- **`delete()`** throws `NoSuchFileException` / `DirectoryNotEmptyException` instead of silently succeeding; it no longer
  recursively deletes a non-empty directory. `deleteIfExists` behaves accordingly.
- **`createDirectory()`** throws `FileAlreadyExistsException` if an object already exists at the key.
- **`move()`** throws `AtomicMoveNotSupportedException` for `ATOMIC_MOVE`.
- **`newDirectoryStream()`** throws `NotDirectoryException` when the path is a regular object, and `DirectoryStream`
  now throws `IllegalStateException` if `iterator()` is called more than once.
- **`getFileStore()` / `FileSystem.getFileStores()`** return a real, non-null `S3FileStore` (bucket-backed) instead of
  `null` / an empty set.
- **`readAttributes(path, "...")`** returns real attributes for directories (with `isDirectory == true`) instead of an
  empty map.
- **`checkAccess()`** throws `AccessDeniedException` for `EXECUTE` (S3 objects are never executable) and for `WRITE`
  on a read-only file system, rather than only logging a warning.
- **Channels:** `FileChannel.read(dst, position)` no longer moves the channel position; `SeekableByteChannel.truncate`
  is supported on writable channels (and throws `NonWritableChannelException` on read-only channels); `DELETE_ON_CLOSE`
  no longer leaves an object in S3.
- **`newFileSystem` / `getFileSystem` / `getPath`:**
  - `getFileSystem(uri)` now throws `FileSystemNotFoundException` when no file system has been created for that bucket
    (previously it lazily created one). The headline `Paths.get(URI)` / `provider.getPath(URI)` ergonomics are
    unchanged — they still materialize a file-system view on demand — so most code is unaffected. Code that relied on
    `getFileSystem` to *create* a file system should call `getPath(uri).getFileSystem()` (or `newFileSystem`) instead.
  - `newFileSystem(uri, env)` gates `FileSystemAlreadyExistsException` on whether a file system for the URI already
    exists in *this JVM* — whether created by a previous `newFileSystem` call or lazily materialized by
    `getPath` / `Paths.get` — not on whether the S3 bucket exists. A pre-existing bucket that you own is reused (fixes
    [#770]); a bucket owned by another account yields `IOException` rather than `FileSystemAlreadyExistsException`.
- **Configuration ([#601], [#597]):**
  - `newFileSystem(uri, env)` now merges the `env` map into the file system's configuration (region, credentials,
    endpoint, timeouts, fragment sizes, etc.), in addition to the existing bucket-creation keys. env-map values take
    precedence over values parsed from the URI. See
    [Per-filesystem configuration](#per-filesystem-configuration-via-newfilesystem).
  - `S3NioSpiConfiguration` now reads `aws.region` and `s3.spi.endpoint` from environment variables and system
    properties (previously ignored). Credentials remain delegated to the AWS SDK chain. When nothing is set,
    `getRegion()` is still `null` and behavior is unchanged.
  - `S3NioSpiConfiguration` no longer extends `HashMap`. Use the `withXxx(...)` setters and getters; a read-only
    snapshot is available via `asMap()`, and bulk overrides via `withOverrides(Map)`. Direct map mutation is no longer
    supported.
  - Removed the deprecated `S3FileSystemProvider.setConfiguration(...)` method and the shared
    `S3FileSystemProvider.configuration` field. Provider operations now read timeouts from each file system's own
    `getConfiguration()`, fixing the last-writer-wins bug when multiple buckets were used ([#597]). Configure per file
    system instead.
  - Removed the unused `S3ClientProvider.universalClient` field.

See [`docs/nio-contract-compliance-audit.md`](docs/nio-contract-compliance-audit.md) and
[`docs/newfilesystem-contract-fix.md`](docs/newfilesystem-contract-fix.md) for the full analysis and rationale.

[#597]: https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/597
[#601]: https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/601

[#770]: https://github.com/awslabs/aws-java-nio-spi-for-s3/pull/770

## Building this library

The library uses the gradle build system and targets Java 11 to allow it to be used in many contexts. To build you can simply run:

```shell
./gradlew build
```

This will run all unit tests and then generate a jar file in `libs` with the name `s3fs-spi-<version>.jar`. Note that
although the compiled JAR targets Java 11 a later version of the JDK may be needed to run Gradle itself.

### Shadowed Jar with dependencies

To build a "fat" jar with the required dependencies (including aws s3 client libraries) you can run:

```shell
./gradlew shadowJar
```

which will produce `s3fs-spi-<version>-all.jar`. If you are using this library as an extension, this is the recommended
jar to use. Don't put both jars on your classpath or extension path, you will observe class conflicts.

## Testing

### Unit Tests

We use [JUnit 5](https://junit.org/junit5/), [AssertJ](https://assertj.github.io/doc/) and [Mockito](https://site.mockito.org/)
for unit testing.

When contributing code for bug fixes or feature improvements, matching tests should also be provided. Tests must not
rely on specific S3 bucket access or credentials. To this end, S3 clients and other artifacts should be mocked as
necessary. Remember, you are testing this library, not the behavior of S3. If you wish to do that you may want to write
an integration test.

Run unit tests with `./gradlew test`

### Integration Tests

Integration tests emulate S3 behavior using [localstack](https://github.com/localstack/localstack). 
Running tests requires a container runtime such as Docker or Podman.

Run integration tests with `./gradlew integrationTest`

Produce code coverage reports with `./gradlew testFullCodeCoverageReport`

HTML output of the reports can be found at:

| Type        | Test Report | Coverage Report                                                        |
|-------------|--------------|------------------------------------------------------------------------|
| Unit        | build/reports/tests/test/index.html             | build/reports/jacoco/testCodeCoverageReport/html/index.html            |
| Integration | build/reports/tests/integrationTest/index.html             | build/reports/jacoco/integrationTestCodeCoverageReport/html/index.html |
| Full | - | build/reports/jacoco/testFullCodeCoverageReport/html/index.html        |

HTML output of the test reports can be found at `build/reports/tests/test/index.html` and test coverage reports are
found at `build/reports/jacoco/test/html/index.html`

## Feedback

We always want to hear more about how people use the package, what they like about it and how we can improve. [Your feedback](https://github.com/awslabs/aws-java-nio-spi-for-s3/discussions/606) helps us build a better product.

## Contributing

We encourage community contributions via pull requests. Please refer to our [code of conduct](./CODE_OF_CONDUCT.md) and
[contributing](./CONTRIBUTING.md) for guidance.

Code must compile to JDK 11 compatible bytecode. Matching unit tests are required for new features and fixes.

