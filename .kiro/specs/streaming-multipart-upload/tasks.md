# Tasks

## Task 1: Create S3StreamingMultipartUpload Open Option

- [x] 1.1 Create `S3StreamingMultipartUpload` class extending `S3OpenOption` with constants (MIN_PART_SIZE, MAX_PART_SIZE, DEFAULT_PART_SIZE, MAX_PARTS, DEFAULT_MAX_IN_FLIGHT), configurable partSize and maxInFlight fields, and a `copy()` method
- [x] 1.2 Add `streamingMultipartUpload()` factory method to `S3OpenOption` returning default instance (8 MiB part size, 4 max in-flight)
- [x] 1.3 Add `streamingMultipartUpload(long partSize)` factory method to `S3OpenOption` with validation: throw `IllegalArgumentException` if partSize < 5 MiB or > 5 GiB
- [x] 1.4 Write unit tests for the open option class: factory methods, validation, defaults, copy behavior

## Task 2: Create PartBuffer Helper Class

- [x] 2.1 Create `PartBuffer` class with a `ByteBuffer` of configurable size, `write(ByteBuffer src)` method that copies bytes into the buffer and returns bytes written, `isFull()`, `flip()`, and `remaining()` methods
- [x] 2.2 Write unit tests for PartBuffer: write fills buffer, isFull detection, boundary writes, flip prepares for reading

## Task 3: Implement S3StreamingMultipartUploadChannel Core

- [x] 3.1 Create `S3StreamingMultipartUploadChannel` implementing `SeekableByteChannel` with mode enum (APPEND_ONLY, RANDOM_ACCESS), position tracking, and open/closed state management
- [x] 3.2 Implement `write(ByteBuffer src)`: on first write call `CreateMultipartUpload`, accumulate bytes in PartBuffer, trigger async upload when buffer is full, assign sequential part numbers, store CompletedPart ETags
- [x] 3.3 Implement `close()` for append-only mode: flush remaining buffer as final part, call `CompleteMultipartUpload` with all part ETags in order, make close idempotent
- [x] 3.4 Implement `position()` and `size()`: position returns total bytes written, size equals position in append-only mode
- [x] 3.5 Implement `read(ByteBuffer dst)`: throw `NonReadableChannelException` (write-only channel)
- [x] 3.6 Implement `truncate(long size)`: throw `UnsupportedOperationException`

## Task 4: Implement Backpressure and Concurrency

- [x] 4.1 Add a `Semaphore` with `maxInFlight` permits to control concurrent part uploads; acquire permit before upload, release on completion
- [x] 4.2 Implement blocking behavior: when all permits are taken, `write()` blocks until an in-flight upload completes and releases a permit
- [x] 4.3 Implement async failure propagation: track failed futures, check for failures at the start of each `write()` and on `close()`, propagate as `IOException`
- [x] 4.4 Write unit tests for backpressure: verify writes block at max in-flight, verify failure propagation

## Task 5: Implement Fallback to Temp-File Mode

- [x] 5.1 Implement `position(long newPosition)`: if newPosition < current position, trigger fallback; otherwise update position normally
- [x] 5.2 Implement fallback logic: abort active multipart session, create temp file, write all previously buffered/uploaded data (from in-memory completed parts data) to temp file, switch mode to RANDOM_ACCESS
- [x] 5.3 Implement random-access mode write/close: delegate writes to temp file channel, on close upload temp file via `S3TransferUtil.uploadLocalFile()`, report size from temp file
- [x] 5.4 Log a warning when fallback occurs including the upload ID
- [x] 5.5 Write unit tests for fallback: backward seek triggers abort, temp file contains correct data, subsequent writes go to temp file, close uploads temp file

## Task 6: Implement Error Handling and Cleanup

- [x] 6.1 Implement abort on part upload failure: catch exceptions from async uploads, call `AbortMultipartUpload`, log upload ID and successful part count
- [x] 6.2 Implement close failure handling: if `CompleteMultipartUpload` fails, attempt abort then throw `IOException`
- [x] 6.3 Register JVM shutdown hook on session initiation to call `AbortMultipartUpload` if session is still active; deregister on successful close
- [x] 6.4 Implement timeout retry: on part upload timeout, retry once before aborting
- [x] 6.5 Handle abort failure gracefully: log warning with upload ID for manual cleanup
- [x] 6.6 Write unit tests for error handling: upload failure triggers abort, close failure aborts, shutdown hook registration, timeout retry, abort failure logging

## Task 7: Implement Part Limit Enforcement

- [x] 7.1 Before initiating each part upload, check if `nextPartNumber > MAX_PARTS`; if so, abort the session and throw `IllegalStateException` with descriptive message including configured part size and total bytes written
- [x] 7.2 Write unit tests for part limit: verify exception at 10,001st part, verify abort is called, verify error message content

## Task 8: Integrate with S3SeekableByteChannel

- [x] 8.1 Modify `S3SeekableByteChannel` constructor to detect `S3StreamingMultipartUpload` option: validate CRT client (throw `UnsupportedOperationException` if not CRT), validate no READ option (throw `IllegalArgumentException`), create `S3StreamingMultipartUploadChannel` as write delegate
- [x] 8.2 Handle option combinations: `assumeObjectNotExists` skips download and sets If-None-Match on complete; `CREATE_NEW` checks existence before session; `useTransferManager` is overridden by streaming option
- [x] 8.3 Implement `force()` support: complete current multipart session, start new session for subsequent writes
- [x] 8.4 Write unit tests for integration: channel creation with various option combinations, CRT validation, force() behavior

## Task 9: Write Property-Based Tests

- [x] 9.1 Add jqwik dependency to build.gradle test dependencies
- [x] 9.2 Write property test: Part size validation — for any long value, factory accepts iff value in [5 MiB, 5 GiB]
- [x] 9.3 Write property test: Buffering threshold — for any write sequence, upload triggers at exactly part size
- [x] 9.4 Write property test: Sequential part ordering — for any multi-part upload, parts numbered 1..N in order
- [x] 9.5 Write property test: Close flushes remaining — for any writes where total % partSize ≠ 0, final part is uploaded
- [x] 9.6 Write property test: Fallback data preservation — for any writes + backward seek, temp file matches written data
- [x] 9.7 Write property test: Close idempotence — for any state, multiple close() calls produce same API calls as one
- [x] 9.8 Write property test: Position tracking — for any write sequence, position() == sum of bytes written
- [x] 9.9 Write property test: Part limit enforcement — for writes exceeding 10,000 parts, IllegalStateException thrown
- [x] 9.10 Write property test: Memory bound — for any writes, buffer count ≤ maxInFlight + 1

## Task 10: Write Integration Tests

- [x] 10.1 Write integration test: streaming upload of multi-part object, read back and verify content matches
- [x] 10.2 Write integration test: fallback on backward seek produces correct object content
- [x] 10.3 Write integration test: channel with default options uploads successfully
- [x] 10.4 Write integration test: force() persists data mid-stream

## Task 11: Update README Documentation

- [x] 11.1 Add "Streaming Multipart Upload" section to README describing the feature, CRT requirement, and when to use it
- [x] 11.2 Document configuration options: part size, max in-flight uploads, and their defaults
- [x] 11.3 Add code example showing how to open a channel with `S3OpenOption.streamingMultipartUpload()`
- [x] 11.4 Document fallback behavior on backward seek
- [x] 11.5 Document S3 part limits (10,000 parts max, 5 GiB max part, 5 MiB min part) and maximum uploadable object size calculation

## Task 12: Create Example Application

- [x] 12.1 Create `StreamingMultipartUploadDemo.java` in `src/examples/java/software/amazon/nio/spi/examples/` demonstrating: opening a channel with streaming option, writing data in a loop, configuring custom part size, with inline comments explaining each step

## Task 13: Add Configuration Properties to S3NioSpiConfiguration

- [x] 13.1 Add constants `S3_SPI_WRITE_STREAMING_MULTIPART_UPLOAD_PROPERTY` (`s3.spi.write.streaming-multipart-upload`, default `false`) and `S3_SPI_WRITE_MULTIPART_PART_SIZE_PROPERTY` (`s3.spi.write.multipart-part-size`, default `8388608`) to `S3NioSpiConfiguration`
- [x] 13.2 Register both properties in the constructor defaults map (before the env var / system property override loops) so they participate in the standard override chain: defaults → env var → system property → programmatic
- [x] 13.3 Add `isStreamingMultipartUploadEnabled()` getter that parses the resolved boolean value, and `getMultipartPartSize()` getter that parses the resolved long value (logging a warning and returning default on parse failure)
- [x] 13.4 Add `parseLongProperty(String propName, long defaultVal)` private helper method analogous to the existing `parseIntProperty`
- [x] 13.5 Add fluent setter `withStreamingMultipartUpload(boolean enabled)` and `withMultipartPartSize(long partSize)` (with validation: partSize must be in [5 MiB, 5 GiB])
- [x] 13.6 Modify `getOpenOptions()` to conditionally add `S3OpenOption.streamingMultipartUpload(getMultipartPartSize())` to the returned set when `isStreamingMultipartUploadEnabled()` returns `true`
- [x] 13.7 Write unit tests: verify default values, verify env var override, verify system property override, verify programmatic override, verify `getOpenOptions()` includes streaming option when enabled, verify `getOpenOptions()` excludes streaming option when disabled, verify invalid part size logs warning and uses default
- [x] 13.8 Write property-based test: for any combination of override sources (env var, system property, programmatic), `getOpenOptions()` includes `S3StreamingMultipartUpload` iff the resolved value of `s3.spi.write.streaming-multipart-upload` is `true`, configured with the resolved part size
