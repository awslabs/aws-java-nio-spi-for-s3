# Requirements Document

## Introduction

This feature introduces streaming multipart upload support to the AWS Java NIO SPI for S3 library. Currently, all write operations buffer the entire content to a local temporary file and upload it to S3 only when the channel is closed. This approach is memory-inefficient and introduces latency for large files, as no data reaches S3 until the channel is closed.

Streaming multipart upload allows parts to be uploaded to S3 as data arrives, significantly reducing memory usage and improving upload latency for large objects. This feature is available when the AWS CRT (Common Runtime) client is in use and the channel is opened in append-only mode. When random-access writes are detected (backward seeks), the system falls back to the existing temp-file approach since multipart uploads require sequential part ordering.

## Glossary

- **Streaming_Upload_Channel**: A write channel implementation that uploads parts to S3 incrementally as data is written, rather than buffering all data locally before uploading.
- **Part_Buffer**: An in-memory buffer that accumulates written bytes until it reaches the configured part size threshold, at which point it is uploaded as a single multipart upload part.
- **Part_Size**: The configurable size threshold (in bytes) at which a filled Part_Buffer is uploaded to S3 as one part of a multipart upload. The minimum is 5 MiB (S3 requirement); the default is 8 MiB.
- **Multipart_Upload_Session**: The lifecycle of a single S3 multipart upload, from initiation (CreateMultipartUpload) through part uploads (UploadPart) to completion (CompleteMultipartUpload) or abort (AbortMultipartUpload).
- **Append_Only_Mode**: A write mode where data is only appended sequentially; the position advances forward monotonically and no backward seeks or overwrites of previously written data occur.
- **Random_Access_Mode**: A write mode where the position may move backward (via seek) and previously written data may be overwritten, requiring the full file to be available locally before upload.
- **CRT_Client**: The AWS Common Runtime-based S3 async client (`S3CrtAsyncClient`) that provides high-performance multipart upload capabilities.
- **S3_Open_Option**: A custom `OpenOption` implementation that configures behavior when opening channels to S3 objects.
- **Fallback**: The process of switching from streaming multipart upload mode to the existing temp-file buffering approach when a backward seek is detected.
- **S3_Writable_Byte_Channel**: The existing channel implementation that buffers all writes to a local temp file and uploads on close.
- **Example_Application**: A standalone Java class in the examples source set that demonstrates how to use the streaming multipart upload feature.
- **README**: The project's top-level README.md documentation file that describes library features, configuration, and usage.

## Requirements

### Requirement 1: Streaming Multipart Upload Open Option

**User Story:** As a developer, I want to opt in to streaming multipart uploads via an open option, so that I can control when the library uses streaming uploads versus the traditional temp-file approach.

#### Acceptance Criteria

1. THE S3_Open_Option class SHALL provide a factory method that returns a streaming multipart upload option instance.
2. WHEN the streaming multipart upload option is included in the open options set, THE S3FileSystemProvider SHALL create a Streaming_Upload_Channel instead of the default S3_Writable_Byte_Channel.
3. WHEN the streaming multipart upload option is included without the CRT_Client being in use, THE S3FileSystemProvider SHALL throw an `UnsupportedOperationException` with a message indicating that CRT is required.
4. WHEN the streaming multipart upload option is combined with `StandardOpenOption.READ`, THE S3FileSystemProvider SHALL throw an `IllegalArgumentException` indicating that streaming upload is write-only.

### Requirement 2: Part Buffering and Upload

**User Story:** As a developer, I want written data to be uploaded to S3 in parts as it accumulates, so that large files do not consume excessive local disk space or memory.

#### Acceptance Criteria

1. THE Streaming_Upload_Channel SHALL accumulate written bytes in a Part_Buffer until the Part_Size threshold is reached.
2. WHEN the Part_Buffer reaches the Part_Size threshold, THE Streaming_Upload_Channel SHALL initiate an asynchronous upload of that part to S3.
3. THE Streaming_Upload_Channel SHALL support a configurable Part_Size with a minimum of 5 MiB and a default of 8 MiB.
4. WHEN a write spans the Part_Size boundary, THE Streaming_Upload_Channel SHALL fill the current Part_Buffer, upload it, and place remaining bytes into a new Part_Buffer.
5. THE Streaming_Upload_Channel SHALL assign sequential part numbers starting from 1 to each uploaded part.

### Requirement 3: Multipart Upload Lifecycle Management

**User Story:** As a developer, I want the multipart upload session to be managed automatically, so that I do not need to handle S3 multipart API calls directly.

#### Acceptance Criteria

1. WHEN the first write occurs on the Streaming_Upload_Channel, THE Streaming_Upload_Channel SHALL initiate a Multipart_Upload_Session by calling CreateMultipartUpload.
2. WHEN the channel is closed successfully, THE Streaming_Upload_Channel SHALL upload any remaining bytes in the Part_Buffer as the final part and call CompleteMultipartUpload with all part ETags.
3. IF an error occurs during any part upload, THEN THE Streaming_Upload_Channel SHALL call AbortMultipartUpload to clean up the incomplete upload.
4. IF the channel is closed without any writes having occurred, THEN THE Streaming_Upload_Channel SHALL not initiate a Multipart_Upload_Session.
5. WHEN CompleteMultipartUpload is called, THE Streaming_Upload_Channel SHALL include all part numbers and their corresponding ETags in sequential order.

### Requirement 4: Fallback to Temp-File on Random Access

**User Story:** As a developer, I want the channel to automatically fall back to the temp-file approach when I perform random-access writes, so that my code works correctly regardless of write patterns.

#### Acceptance Criteria

1. WHEN a position is set to a value less than the current write position on the Streaming_Upload_Channel, THE Streaming_Upload_Channel SHALL abort the active Multipart_Upload_Session.
2. WHEN a backward seek is detected, THE Streaming_Upload_Channel SHALL transition to Random_Access_Mode by creating a local temp file and downloading any previously uploaded parts into it.
3. WHILE in Random_Access_Mode, THE Streaming_Upload_Channel SHALL behave identically to the existing S3_Writable_Byte_Channel (buffering to temp file and uploading on close).
4. WHEN the Fallback occurs, THE Streaming_Upload_Channel SHALL log a warning message indicating the transition from streaming to temp-file mode.
5. WHEN the Fallback occurs after parts have been uploaded, THE Streaming_Upload_Channel SHALL reconstruct the local temp file by downloading the object prefix using the already-uploaded parts data held in memory.

### Requirement 5: Channel Close and Completion

**User Story:** As a developer, I want the channel close operation to finalize the upload reliably, so that my data is fully persisted to S3 when close() returns.

#### Acceptance Criteria

1. WHEN close() is called in Append_Only_Mode, THE Streaming_Upload_Channel SHALL flush the remaining Part_Buffer content as the final part and complete the Multipart_Upload_Session.
2. WHEN close() is called in Random_Access_Mode, THE Streaming_Upload_Channel SHALL upload the temp file using the existing S3TransferUtil upload mechanism.
3. IF close() fails during CompleteMultipartUpload, THEN THE Streaming_Upload_Channel SHALL attempt to abort the Multipart_Upload_Session and throw an IOException.
4. THE Streaming_Upload_Channel SHALL be idempotent on close: calling close() multiple times SHALL have no additional effect after the first successful close.
5. WHEN the final part is smaller than the Part_Size, THE Streaming_Upload_Channel SHALL upload it as the last part (S3 allows the final part to be smaller than 5 MiB).

### Requirement 6: Concurrency and Backpressure

**User Story:** As a developer, I want the channel to handle concurrent part uploads efficiently without unbounded memory growth, so that the system remains stable under heavy write loads.

#### Acceptance Criteria

1. THE Streaming_Upload_Channel SHALL upload parts asynchronously, allowing the next Part_Buffer to fill while the previous part is being uploaded.
2. WHILE the number of in-flight part uploads reaches a configurable maximum (default: 4), THE Streaming_Upload_Channel SHALL block subsequent writes until an in-flight upload completes.
3. IF an in-flight part upload fails, THEN THE Streaming_Upload_Channel SHALL propagate the failure on the next write or close operation.
4. THE Streaming_Upload_Channel SHALL limit total memory consumption to approximately (max_in_flight + 1) multiplied by Part_Size.

### Requirement 7: Part Size Configuration

**User Story:** As a developer, I want to configure the part size for streaming uploads, so that I can tune performance for my specific workload.

#### Acceptance Criteria

1. THE S3_Open_Option class SHALL provide a factory method that accepts a Part_Size parameter for the streaming multipart upload option.
2. WHEN a Part_Size less than 5 MiB is specified, THE S3_Open_Option factory method SHALL throw an `IllegalArgumentException` indicating the minimum part size.
3. WHEN a Part_Size greater than 5 GiB is specified, THE S3_Open_Option factory method SHALL throw an `IllegalArgumentException` indicating the maximum part size.
4. WHEN no Part_Size is specified, THE Streaming_Upload_Channel SHALL use a default Part_Size of 8 MiB.

### Requirement 8: Integration with Existing Open Options

**User Story:** As a developer, I want streaming multipart upload to work alongside existing S3 open options, so that I can combine features like integrity checks and conditional writes.

#### Acceptance Criteria

1. WHEN the streaming multipart upload option is combined with `S3OpenOption.assumeObjectNotExists()`, THE Streaming_Upload_Channel SHALL skip any initial download and set the `If-None-Match` header on the CompleteMultipartUpload request.
2. WHEN the streaming multipart upload option is combined with `StandardOpenOption.CREATE_NEW`, THE Streaming_Upload_Channel SHALL verify the object does not exist before initiating the Multipart_Upload_Session.
3. WHEN the streaming multipart upload option is combined with `S3OpenOption.useTransferManager()`, THE Streaming_Upload_Channel SHALL take precedence (streaming upload is used instead of transfer manager for the upload path).
4. THE Streaming_Upload_Channel SHALL support the `force()` operation by completing the current Multipart_Upload_Session and starting a new one for subsequent writes.

### Requirement 9: Error Handling and Cleanup

**User Story:** As a developer, I want failed uploads to be cleaned up automatically, so that incomplete multipart uploads do not accumulate in my S3 bucket and incur storage costs.

#### Acceptance Criteria

1. IF the JVM terminates abnormally while a Multipart_Upload_Session is active, THEN THE Streaming_Upload_Channel SHALL register a shutdown hook to attempt AbortMultipartUpload.
2. IF a timeout occurs during a part upload, THEN THE Streaming_Upload_Channel SHALL retry the part upload once before aborting the session.
3. WHEN an abort is performed, THE Streaming_Upload_Channel SHALL log the upload ID and the number of parts that were successfully uploaded before the failure.
4. IF AbortMultipartUpload itself fails, THEN THE Streaming_Upload_Channel SHALL log a warning with the upload ID so the user can manually clean up the incomplete upload.

### Requirement 10: Position and Size Tracking

**User Story:** As a developer, I want accurate position and size reporting from the channel, so that my code can make decisions based on how much data has been written.

#### Acceptance Criteria

1. THE Streaming_Upload_Channel SHALL report the current write position as the total number of bytes written since the channel was opened.
2. THE Streaming_Upload_Channel SHALL report the channel size as equal to the current write position in Append_Only_Mode.
3. WHILE in Random_Access_Mode, THE Streaming_Upload_Channel SHALL report size based on the underlying temp file size.
4. WHEN position() is called, THE Streaming_Upload_Channel SHALL return the value without blocking on in-flight uploads.

### Requirement 11: S3 Part Limits Enforcement

**User Story:** As a developer, I want the streaming upload channel to enforce S3's hard limits on part count and part size, so that uploads never fail due to exceeding S3 service constraints.

#### Acceptance Criteria

1. THE Streaming_Upload_Channel SHALL reject uploads that would exceed 10,000 parts by throwing an `IllegalStateException` with a message indicating the S3 maximum part limit has been reached.
2. WHEN the configured Part_Size exceeds 5 GiB, THE S3_Open_Option factory method SHALL throw an `IllegalArgumentException` indicating the maximum S3 part size.
3. WHEN the total data written would require more than 10,000 parts at the configured Part_Size, THE Streaming_Upload_Channel SHALL throw an `IllegalStateException` before initiating the part that would exceed the limit.
4. IF the part number counter reaches 10,000, THEN THE Streaming_Upload_Channel SHALL abort the active Multipart_Upload_Session and throw an `IllegalStateException` with a descriptive error message including the configured Part_Size and total bytes written.

### Requirement 12: README Documentation

**User Story:** As a developer, I want the README to document streaming multipart upload support, so that I can understand how to configure and use the feature without reading source code.

#### Acceptance Criteria

1. THE README SHALL include a section describing how streaming multipart uploads are supported, including the requirement for the CRT_Client.
2. THE README SHALL document the configuration options for streaming multipart upload, including Part_Size and maximum in-flight uploads.
3. THE README SHALL provide a code example demonstrating how to open a channel with the streaming multipart upload option.
4. THE README SHALL document the fallback behavior that occurs when a backward seek is detected during a streaming upload.
5. THE README SHALL document the S3 part limits (maximum 10,000 parts, maximum 5 GiB per part, minimum 5 MiB per part) and how they affect the maximum uploadable object size.

### Requirement 13: Example Application

**User Story:** As a developer, I want a working example application demonstrating streaming multipart upload, so that I can quickly understand how to integrate the feature into my own code.

#### Acceptance Criteria

1. THE Example_Application SHALL be located in the examples source set at `src/examples/java/software/amazon/nio/spi/examples/`.
2. THE Example_Application SHALL demonstrate opening a channel with the streaming multipart upload open option.
3. THE Example_Application SHALL demonstrate writing data to S3 using the Streaming_Upload_Channel in a loop to illustrate incremental part uploads.
4. THE Example_Application SHALL demonstrate configuring a custom Part_Size via the S3_Open_Option factory method.
5. THE Example_Application SHALL include inline comments explaining each step of the streaming upload process.

### Requirement 14: Environment Variable and System Property Configuration for SPI Users

**User Story:** As an SPI user (e.g., GATK), I want to enable streaming multipart uploads via environment variables or system properties, so that I can activate the feature without modifying application code or passing custom open options programmatically.

#### Acceptance Criteria

1. THE S3NioSpiConfiguration SHALL define a property `s3.spi.write.streaming-multipart-upload` that accepts a boolean string value (`true` or `false`) with a default of `false`.
2. THE S3NioSpiConfiguration SHALL define a property `s3.spi.write.multipart-part-size` that accepts a long value in bytes with a default of 8388608 (8 MiB).
3. WHEN the environment variable `S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD` is set to `true`, THE S3NioSpiConfiguration SHALL override the default value of the `s3.spi.write.streaming-multipart-upload` property.
4. WHEN the system property `s3.spi.write.streaming-multipart-upload` is set to `true`, THE S3NioSpiConfiguration SHALL override the environment variable value of the `s3.spi.write.streaming-multipart-upload` property.
5. WHEN the environment variable `S3_SPI_WRITE_MULTIPART_PART_SIZE` is set, THE S3NioSpiConfiguration SHALL override the default value of the `s3.spi.write.multipart-part-size` property.
6. WHEN the system property `s3.spi.write.multipart-part-size` is set, THE S3NioSpiConfiguration SHALL override the environment variable value of the `s3.spi.write.multipart-part-size` property.
7. WHEN `s3.spi.write.streaming-multipart-upload` resolves to `true`, THE S3NioSpiConfiguration SHALL include an `S3StreamingMultipartUpload` option (configured with the resolved part size) in the set returned by `getOpenOptions()`.
8. WHEN `s3.spi.write.streaming-multipart-upload` resolves to `false`, THE S3NioSpiConfiguration SHALL not include an `S3StreamingMultipartUpload` option in the set returned by `getOpenOptions()`.
9. WHEN `s3.spi.write.multipart-part-size` contains a non-numeric value, THE S3NioSpiConfiguration SHALL log a warning and use the default value of 8388608 bytes.
10. THE S3NioSpiConfiguration SHALL apply overrides in the standard order: defaults, then environment variable, then system property, then programmatic (via constructor overrides map).
