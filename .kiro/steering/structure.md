# Project Structure

## Source Organization
```
src/
├── main/java/software/amazon/nio/spi/s3/     # Core implementation
├── test/java/software/amazon/nio/spi/s3/     # Unit tests
├── integrationTest/java/software/amazon/nio/spi/s3/  # Integration tests
└── examples/java/software/amazon/nio/spi/s3/examples/  # Usage examples
```

## Package Structure
- **Root package**: `software.amazon.nio.spi.s3`
- **Config package**: `software.amazon.nio.spi.s3.config` - Configuration classes
- **Util package**: `software.amazon.nio.spi.s3.util` - Utility classes
- **Examples package**: `software.amazon.nio.spi.s3.examples` - Demo applications

## Key Components

### Core Classes
- `S3FileSystemProvider` - Main SPI implementation
- `S3FileSystem` - Represents an S3 bucket as a filesystem
- `S3Path` - S3 object path implementation
- `S3SeekableByteChannel` - Read operations with caching
- `S3WritableByteChannel` - Write operations via temp files

### Channel Implementations
- `S3ReadAheadByteChannel` - Async read-ahead caching
- `S3FileChannel` - Standard file channel operations
- `AsyncS3FileChannel` - Asynchronous file operations

### Utility Classes
- `PosixLikePathRepresentation` - Path semantics mapping
- `S3ClientProvider` - S3 client management
- `TimeOutUtils` - Timeout handling utilities

## Naming Conventions
- Classes prefixed with `S3` for core functionality
- Test classes suffixed with `Test`
- Integration tests in separate source set
- Examples use descriptive action names (e.g., `CopyToS3`, `ListPrefix`)

## Configuration Files
- `src/main/resources/META-INF/services/` - SPI registration
- `.checkstyle/` - Code style configuration
- `gradle/` - Gradle wrapper and configuration

## Test Structure
- Unit tests mock S3 operations and focus on logic
- Integration tests use Testcontainers with LocalStack
- Examples demonstrate real-world usage patterns
- Test utilities in `S3Matchers` and `FixedS3ClientProvider`

## Build Artifacts
- `build/libs/` - Generated JAR files
- `build/reports/` - Test and coverage reports
- `build/staging-deploy/` - Maven publication staging