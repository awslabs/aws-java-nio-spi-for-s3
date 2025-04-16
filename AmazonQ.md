# AWS Java NIO SPI for S3 - Project Summary

## Project Overview
The AWS Java NIO SPI for S3 is a Java NIO.2 service provider implementation that enables Java applications to interact with Amazon S3 using standard Java NIO file operations. It implements the service provider interface (SPI) defined for Java NIO.2 in JDK 1.7, allowing applications to access S3 objects without modification or recompilation.

## Key Features
- Enables Java NIO.2 operations on Amazon S3 using the `s3://` URI scheme
- Supports S3-compatible endpoints via the `s3x://` URI scheme


## Java NIO Filesystem SPI Concepts
The Java NIO.2 Filesystem SPI (Service Provider Interface) introduced in Java 7 provides a framework for implementing custom filesystem providers. Key concepts include:

- **FileSystemProvider**: The entry point for a filesystem implementation that handles creation of filesystems and paths
  - Implemented by `S3FileSystemProvider` for s3:// URIs and `S3XFileSystemProvider` for s3x:// URIs
- **FileSystem**: Represents a filesystem and provides factory methods for paths and operations on the filesystem
  - Implemented by `S3FileSystem` where each bucket is represented as a separate filesystem
- **Path**: Represents a location in a filesystem, analogous to a file or directory path
  - Implemented by `S3Path` with path representation handled by `PosixLikePathRepresentation`
- **Files**: Utility class with static methods for file operations that delegate to the appropriate provider
  - Standard Java `Files` class is used, with operations delegated to the S3 providers
- **FileStore**: Represents the underlying storage for a filesystem
  - Not explicitly implemented as a separate class; bucket information is managed within `S3FileSystem`
- **WatchService**: Enables watching for changes to files or directories
  - Not implemented as S3 doesn't support native file watching capabilities
- **SeekableByteChannel**: Provides random access read/write operations on files
  - Implemented by `S3SeekableByteChannel` with read operations delegated to `S3ReadAheadByteChannel` and write operations to `S3WritableByteChannel`
- **DirectoryStream**: Enables iteration over directory entries
  - Implemented by `S3DirectoryStream` for listing objects in S3 "directories"

The SPI design allows applications to use the same API for different storage systems by loading appropriate providers.

## Project Structure
- Main implementation in `software.amazon.nio.s3` package
- Examples provided in the `examples` module
- Uses Gradle as the build system
- Targets Java 11+

## Core Components

### S3 Path Representation
- Buckets are represented as `FileSystem` instances
- S3 objects are represented as `Path` instances
- Uses "posix-like" path representations for directory inference

### Reading Files
- Implements `S3SeekableByteChannel` for reading S3 objects
- Uses `S3ReadAheadByteChannel` with an in-memory cache for optimized sequential reads
- Configurable fragment size and number for read-ahead caching

### Writing Files
- Writes are buffered in a temporary file and uploaded to S3 on channel close
- Channels can be used either for read or write operations, but not both

## Configuration Options
- Fragment size: `s3.spi.read.fragment-size` or `S3_SPI_READ_MAX_FRAGMENT_SIZE`
- Fragment number: `s3.spi.read.fragment-number` or `S3_SPI_READ_MAX_FRAGMENT_NUMBER`
- S3 endpoint protocol: `s3.spi.endpoint-protocol` or `S3_SPI_ENDPOINT_PROTOCOL`
- Standard AWS credentials configuration via default credential provider chain

## Design Decisions and Limitations
- Directories are inferred, not actual S3 objects
- No support for symbolic links
- Creation time and last modified time are identical
- No concept of hidden files
- Working directory is always the root
- Special path symbols `.` and `..` are resolved as in POSIX paths
- `deleteIfExists()` always returns true due to S3 API limitations
- Directory copies include all contents via batched operations

## Integration Patterns
1. Include as a compile dependency
2. Include on classpath at runtime (Java 9+)

## AWS Credentials
Uses the AWS SDK for Java default credential provider chain. No library-specific credential configuration is supported.

## Testing
- Unit tests use JUnit 5, AssertJ, and Mockito
- Integration tests use localstack to emulate S3 behavior
- Requires a container runtime (Docker/Podman) for integration tests

## Important Implementation Notes
- S3 has higher latency compared to local filesystems
- Read operations are optimized for sequential access patterns
- Careful consideration needed for fragment size/number to avoid hitting S3 rate limits
- S3 URI and Java URI incompatibilities with special characters
