# NIO Contract Compliance Audit

> **Status:** All findings in this document were implemented in the `3.0.0-rc1` release candidate.
> The breaking changes are summarized in the README under
> [Breaking changes in 3.0](../README.md#breaking-changes-in-30). Regression tests live in
> `S3PathTest`, `S3FileSystemProviderTest`, `S3FileSystemTest`, `S3FileChannelTest`,
> `S3WritableByteChannelTest`, `S3FileStoreTest`, and `NioContractComplianceTest`.

This document catalogs places where `aws-java-nio-spi-for-s3` diverges from the official
`java.nio.file` contracts (as specified in the JDK javadoc for `Path`, `FileSystem`,
`java.nio.file.spi.FileSystemProvider`, `SeekableByteChannel`, `FileChannel`,
`AsynchronousFileChannel`, `DirectoryStream`, and the `Files` facade), and specifies the
changes recommended to bring the provider into conformance.

Some fixes are **breaking** and are appropriate for a major release. Each finding is tagged
with a severity and a compatibility note. The `newFileSystem` / `getFileSystem` / `getPath`
contract is analysed separately in [`newfilesystem-contract-fix.md`](./newfilesystem-contract-fix.md)
and is only cross-referenced here.

Findings were verified against the source at commit level `2.5.0` (branch `main`). Line
numbers refer to that state and will drift as the code changes.

## How to read this document

- **Severity** — `Critical` (produces wrong results / NPE with no workaround), `High`
  (silent contract inversion or data loss), `Medium` (observable divergence with a
  workaround), `Low` (deviation / stale documentation).
- **Breaking?** — whether the recommended fix changes behavior that existing callers may
  depend on.
- Several findings correspond to open GitHub issues: [#771], [#772], [#773], and PR [#770].

---

## Summary table

| # | Area | Contract issue | Severity | Breaking? | Issue/PR |
|---|------|----------------|----------|-----------|----------|
| 1 | `PosixLikePathRepresentation(char[])` | Constructor never assigns `path`; `relativize()` returns a `null`-backed Path | Critical | No (bug fix) | — |
| 2 | `S3Path.getParent()` | Returns a **relative** path for absolute inputs; throws for single-element relative paths | High | Yes | [#772] |
| 3 | `S3Path.relativize()` | Miscounts the shared prefix, emitting spurious `..` | High | Yes | — |
| 4 | `S3Path.normalize()` | Drops leading `..` in relative paths; can strip the root from absolute paths | High | Yes | — |
| 5 | `S3Path.equals()`/`hashCode()`/`compareTo()` | Normalize via `toRealPath()`, so `.`/`..` are eliminated during equality | High | Yes | — |
| 6 | `S3Path.iterator()` | Emits the root component and trailing separators as name elements | Medium | Yes | — |
| 7 | `S3Path.resolveSibling()` | NPE / `IllegalArgumentException` when there is no parent | Medium | No (bug fix) | — |
| 8 | `FileSystemProvider.delete()` | Never throws `NoSuchFileException`; recursively deletes non-empty "directories" | High | Yes | — |
| 9 | `FileSystemProvider.createDirectory()` | Never throws `FileAlreadyExistsException`; silently overwrites | High | Yes | — |
| 10 | `FileSystemProvider.move()` | Ignores `ATOMIC_MOVE`; never throws `AtomicMoveNotSupportedException` | Medium | Yes | — |
| 11 | `FileSystemProvider.newDirectoryStream()` | Never throws `NotDirectoryException` | Medium | Yes | — |
| 12 | `FileSystemProvider.getFileStore()` | Returns `null`, causing NPE in callers | Medium | Yes | — |
| 13 | `readAttributes(Path, String, …)` | Returns an empty map for directories | Medium | Yes | — |
| 14 | `FileChannel.read(dst, position)` | Mutates the channel position | High | No (bug fix) | — |
| 15 | `AsynchronousFileChannel` positional ops | Share unsynchronized mutable position; races | High | No (bug fix) | — |
| 16 | `S3FileSystem.createTempFile()` | Check-then-act race → spurious `FileAlreadyExistsException` | High | No (bug fix) | [#771] |
| 17 | `truncate()` | Throws `UnsupportedOperationException` even on writable channels | Medium | Yes | — |
| 18 | `DELETE_ON_CLOSE` | Ignored for the S3 object; object is still uploaded | Medium | No (bug fix) | — |
| 19 | `checkAccess()` | `WRITE`/`EXECUTE` are logged, not enforced | Low | No | — |
| 20 | `S3DirectoryStream.iterator()` | Second call does not throw `IllegalStateException` | Low | Yes | — |
| 21 | `S3FileSystemProvider.closeFileSystem()` | Not thread-safe; NPE / redundant close | Medium | No (bug fix) | [#773] |
| 22 | `S3SeekableByteChannel.validateOpen()` | `closed` read without synchronization (visibility) | Low | No (bug fix) | — |
| 23 | Stale javadoc | `readAttributes(Path, Class, …)` claims to return `null` for directories; it does not | Low | No | — |

---

## Path contract (`java.nio.file.Path`)

### 1. `PosixLikePathRepresentation(char[])` never initializes the backing string — Critical

**File:** `PosixLikePathRepresentation.java:39-41`

```java
PosixLikePathRepresentation(char[] path) {
    new PosixLikePathRepresentation(new String(path));   // constructs and discards
}
```

The constructor builds a *throwaway* instance instead of assigning `this.path`. The `path`
field stays `null`. This constructor is used by `S3Path.relativize()` at
`S3Path.java:583` and `S3Path.java:618`, so any `relativize()` result that contains a `..`
component (i.e. whenever `parentDirCount > 0`) is an `S3Path` whose backing string is `null`.

**Verified empirically:** `Paths.get("s3://b/a/b/c/d/").relativize(Paths.get("s3://b/a/b/"))`
returns a path whose `toString()` is `null` and whose `isAbsolute()` is `false`.
`normalize()`, `elements()`, and most other operations then throw `NullPointerException`
(`path.split(...)`).

This defect is currently masked by finding #5: `S3PathTest.relativize` only passes because
`S3Path.equals()` compares the *normalized* form, and `toRealPath()` of a `null`-backed path
happens to reduce to a value equal to the expected result. `toString()` on the returned path
is `null`.

**Fix (non-breaking bug fix):**

```java
PosixLikePathRepresentation(char[] path) {
    this(new String(path));
}
```

Add a direct assertion in `S3PathTest.relativize` that the returned path's `toString()`
equals the expected string (not merely `.equals()`), so the regression cannot recur.

### 2. `getParent()` returns a relative path and later throws — High — Breaking — [#772]

**File:** `S3Path.java:177-187`, via `subpath()` at `S3Path.java:243-265`

For an absolute path with more than one element, `getParent()` falls through to
`subpath(0, getNameCount() - 1)`. `subpath()` joins the name elements but never re-prepends
the leading `/`, so the result is **relative**.

**Verified empirically:** `Paths.get("s3://b/a/b/c").getParent()` returns `a/b/`
(`isAbsolute() == false`). The `Path` contract requires the parent to retain the root
component, i.e. `/a/b`.

Because the root is lost, repeatedly walking `getParent()` toward the root eventually calls
`subpath(0, 0)`, which throws `IllegalArgumentException: end index may not be <= the begin
index`. Additionally, `getParent()` on a **single-element relative** path (e.g. `foo`)
reaches `subpath(0, 0)` and throws instead of returning `null` (see also #7).

This surfaces through `Files.createDirectories(path)`, whose JDK fallback walks ancestors via
`getParent()`.

**Fix (breaking):** Make `getParent()` preserve absoluteness and return `null` at the top:

```java
@Override
public S3Path getParent() {
    var size = pathRepresentation.elements().size();
    if (this.equals(getRoot()) || size == 0) {
        return null;
    }
    if (size == 1) {
        return isAbsolute() ? getRoot() : null;
    }
    var parent = subpath(0, size - 1);
    return isAbsolute() ? (S3Path) parent.toAbsolutePath() : parent;
}
```

or, more robustly, have `subpath()` re-prepend the root when the source path is absolute and
`beginIndex == 0`. Add tests asserting the full parent chain
`/aa/bb/cc/ → /aa/bb/ → /aa/ → /` and that each element stays absolute.

### 3. `relativize()` miscounts the shared prefix — High — Breaking

**File:** `S3Path.java:542-584`, driven by `getDifferenceCount()` at `S3Path.java:621-630`

`getDifferenceCount()` compares `this.getName(i).equals(other.getName(i))`. `getName(i)`
(via `subpath()`) appends a trailing `/` to an element unless it is the *last* element of
that particular path, so the *same* logical element is rendered differently in the two
operands and compares unequal:

- `this = /a/b` → `getName(1)` → last element → `b`
- `other = /a/b/c` → `getName(1)` → not last → `b/`
- `b`.equals(`b/`) → `false`

The shared prefix is therefore undercounted, and `relativize()` emits spurious `..`
components. The JDK's own documented example — `"/a/b".relativize("/a/b/c/d")` must yield
`c/d` — is not honored.

**Fix (breaking):** Compare *bare* name strings (no separator, no normalization) when
computing the difference count, e.g. compare `pathRepresentation.elements().get(i)` values
directly rather than routing through `getName()`/`equals()`. Fix in conjunction with #1 and
#5. Add tests covering the JDK example and the trailing-separator cases.

### 4. `normalize()` drops leading `..` and can strip the root — High — Breaking

**File:** `S3Path.java:392-425`

The `..` branch always discards the `..` after (conditionally) removing the previous element:

```java
if (element.equals("..")) {
    if (!realElements.isEmpty()) { realElements.removeLast(); }
    continue;   // ".." is always dropped
}
```

Two problems:

a. **Leading `..` in a relative path is silently discarded.** The contract states a `..` is
   redundant *only when preceded by a non-`..` name*. **Verified:**
   `Paths.get("s3://b/../foo").normalize()` returns `foo` (the `..` is lost). It should
   return `../foo`.

b. **Absolute paths can lose their root.** The root is stored as a literal `/` element in
   `realElements`, so a `..` at the top pops it, turning an absolute path into a relative
   one (`/../foo` → `foo`). A leading `..` at the root should be dropped without removing the
   root (`/../foo` → `/foo`).

**Fix (breaking):** Track absoluteness separately from the element list (do not store `/` as
an element). Only pop for `..` when the last retained element is a real name (not `..`). For
absolute paths, ignore `..` that would rise above the root. Preserve leading `..` in relative
paths. Add tests for `../foo`, `../../foo`, `/../foo`, `a/../..`, and `a/b/../..`.

### 5. `equals()` / `hashCode()` / `compareTo()` normalize before comparing — High — Breaking

**File:** `equals` `S3Path.java:831-841`; `hashCode` `S3Path.java:852-855`;
`compareTo` `S3Path.java:811-819`

All three compare `toRealPath(NOFOLLOW_LINKS)`, which normalizes (eliminates `.` and `..`).
`Path.equals` requires comparison of the *abstract* path strings and explicitly does **not**
eliminate special names.

**Verified:** `Paths.get("s3://b/a/./b").equals(Paths.get("s3://b/a/b"))` returns `true`
here; in the JDK it is `false`. (Note: the related case `"/a/b/.."` vs `"/a"` currently
returns `false` in this implementation, but by accident of the `normalize()` bug in #4, not
by design — it is not reliable.)

Because `equals` is used pervasively (`startsWith`, `endsWith`, `getDifferenceCount`, cache
keys, `Set`/`Map` membership), the non-conformant normalization contaminates those operations
and contributes to #3.

**Fix (breaking):** Base `equals`/`hashCode`/`compareTo` on the *abstract* path
(`pathRepresentation` plus bucket), not the normalized real path. Callers that need
"same object" semantics should use `isSameFile`/`toRealPath` explicitly. This is a behavioral
change for anyone relying on the current normalized equality, hence major-version only.

### 6. `iterator()` emits the root and trailing separators — Medium — Breaking

**File:** `S3Path.java:904-938` (`S3PathIterator.next`)

The contract states: *"The root component, if present, is not returned by the iterator."*
**Verified:** iterating `/a/b` yields `/a/` then `b` — the first element carries the leading
`/` (root) and a trailing separator, and interior elements are separator-suffixed.

**Fix (breaking):** Return bare name elements (`a`, then `b`) with no root and no trailing
separators, matching the JDK. Keep any S3 "directory" semantics out of the iterator.

### 7. `resolveSibling()` throws when there is no parent — Medium

**File:** `S3Path.java:500-518`

```java
public S3Path resolveSibling(Path other) { return getParent().resolve(other); }
```

The contract says: if this path has no parent, return `other`; if `other` is empty, return
the parent (or the empty path). With no null-guard, calling `resolveSibling` on the empty
path (parent is `null`) throws `NullPointerException`, and on a single-element relative path
it throws `IllegalArgumentException` (see #2).

**Fix (non-breaking):**

```java
@Override
public S3Path resolveSibling(Path other) {
    checkPath(other);
    var parent = getParent();
    if (parent == null || ((S3Path) other).isAbsolute()) {
        return (S3Path) other;
    }
    return parent.resolve(other);
}
```

Also fix the `resolveSibling(String)` overload to route through the same logic.

### Path — not violations (reviewed)

- `register(WatchService, …)` throwing `UnsupportedOperationException` is acceptable (no watch
  service for S3).
- `startsWith(String)` / `endsWith(String)` delegate to the `Path` overloads correctly.
- `toRealPath()` not verifying existence (and not throwing `IOException`) is a deliberate,
  acceptable deviation for a non-default provider — it must stay I/O-free because
  `equals`/`hashCode`/`getKey`/`toUri` depend on it. Documented here for completeness (Low).

---

## Provider / `Files` contract (`java.nio.file.spi.FileSystemProvider`)

### 8. `delete()` never throws `NoSuchFileException`; recursively deletes non-empty directories — High — Breaking

**File:** `S3FileSystemProvider.java:437-469` (with `getContainedObjectBatches` at
`948-977`)

Two contract violations:

a. **Missing file.** `Files.delete` must throw `NoSuchFileException` when the target does not
   exist (this is the whole distinction from `deleteIfExists`). S3 `DeleteObjects` is
   idempotent and returns success for absent keys, so `delete` of a non-existent path returns
   normally. `delete` and `deleteIfExists` become indistinguishable.

b. **Non-empty directory.** `Files.delete` must throw `DirectoryNotEmptyException` for a
   non-empty directory. Instead the code enumerates *all* objects under the prefix and
   deletes the entire subtree — a destructive divergence and a data-loss hazard. The class
   documents a recursive variance for `copy`, but not for `delete`.

**Fix (breaking):**
- For a regular file, `headObject` first and throw `NoSuchFileException` if absent.
- For a directory, list one level; if it contains entries, throw
  `DirectoryNotEmptyException`. Provide the recursive deletion only through an explicit,
  clearly documented API (or keep it, but document the variance as loudly as the `copy`
  variance and gate it behind an option).

### 9. `createDirectory()` never throws `FileAlreadyExistsException` — High — Breaking

**File:** `S3FileSystemProvider.java:399-429`

The contract requires `FileAlreadyExistsException` when an entry of that name already exists.
Except for the root special-case, the method issues `putObject(…, empty())` unconditionally,
silently overwriting any existing object/marker at that key.

**Fix (breaking):** Check for an existing object (or non-empty prefix) at the key and throw
`FileAlreadyExistsException` if present, before writing the directory marker.

### 10. `move()` ignores `ATOMIC_MOVE` — Medium — Breaking

**File:** `S3FileSystemProvider.java:544-548`

`move` is `copy(...)` + `delete(...)` — inherently non-atomic — and the options array is only
inspected for `REPLACE_EXISTING`. `ATOMIC_MOVE` is silently ignored, giving callers a false
atomicity guarantee. The contract requires `AtomicMoveNotSupportedException` when
`ATOMIC_MOVE` is requested but cannot be honored.

**Fix (breaking):** If `options` contains `StandardCopyOption.ATOMIC_MOVE`, throw
`AtomicMoveNotSupportedException`.

### 11. `newDirectoryStream()` never throws `NotDirectoryException` — Medium — Breaking

**File:** `S3FileSystemProvider.java:363-389`

If the path is not a directory, the code just appends `/` to the key and lists with that
prefix, coercing a regular object into an (empty) directory stream. The contract requires
`NotDirectoryException`.

**Fix (breaking):** When the path resolves to a regular object (verify via head), throw
`NotDirectoryException` rather than streaming an empty result.

### 12. `getFileStore()` returns `null` — Medium — Breaking

**File:** `S3FileSystemProvider.java:584-587`

The method is declared to return a `FileStore`; returning `null` causes
`Files.getFileStore(path).name()` (and similar) to `NullPointerException`.

**Fix (breaking):** Return a minimal read-through `FileStore` implementation (name = bucket,
type = `"s3"`, `supportsFileAttributeView("basic") == true`, unlimited/`Long.MAX_VALUE`
space, `isReadOnly() == false`). `S3FileSystem.getFileStores()` should return the same store
rather than an empty set.

### 13. `readAttributes(Path, String, …)` returns an empty map for directories — Medium — Breaking

**File:** `S3FileSystemProvider.java:803-814`

```java
if (s3Path.isDirectory() || attributes.trim().isEmpty()) {
    return Collections.emptyMap();
}
```

A directory is a valid file and should report `isDirectory=true`, `size`, times, etc.
Returning `{}` means `(Boolean) map.get("isDirectory")` is `null` (NPE on unboxing). This
also disagrees with the `Class`-based overload, which *does* return proper directory
attributes (`DIRECTORY_ATTRIBUTES`).

**Fix (breaking):** Return the real (directory) attribute map for directory paths, aligning
the two `readAttributes` overloads.

### 19. `checkAccess()` does not enforce `WRITE`/`EXECUTE` — Low

**File:** `S3FileSystemProvider.java:659-713`

`WRITE`/`EXECUTE` are logged as warnings and then only existence is checked, so
`Files.isWritable(readOnlyObject)` returns `true`. The contract expects the requested modes
to be checked (or `AccessDeniedException` thrown when access cannot be determined).

**Fix:** For `WRITE`/`EXECUTE`, either determine access from bucket/object ACLs or throw
`AccessDeniedException` ("access cannot be determined") rather than silently passing.
Document whichever choice is made.

### 20. `S3DirectoryStream.iterator()` can be called more than once — Low — Breaking

**File:** `S3DirectoryStream.java:40-44`

`iterator()` returns the same cached instance on every call. The contract requires a second
invocation to throw `IllegalStateException`.

**Fix (breaking):** Track whether `iterator()` has been called and throw `IllegalStateException`
on the second call. Also implement `close()` (currently a no-op) to release the underlying
publisher subscription.

### 23. Stale javadoc on `readAttributes(Path, Class, …)` — Low

**File:** `S3FileSystemProvider.java:769-781`

The javadoc claims the method returns `null` "if `path` is inferred to be a directory," but
the code returns `DIRECTORY_ATTRIBUTES` for directories and is contract-compliant. Fix the
javadoc.

### Provider — not violations (reviewed)

- `copy()` honors `REPLACE_EXISTING` and throws `FileAlreadyExistsException` when the target
  exists without it. `COPY_ATTRIBUTES` and unknown options are ignored rather than rejected
  (the contract permits `UnsupportedOperationException`); recursive directory copy is a
  documented intentional variance.
- `getFileAttributeView` returns `null` only for unsupported view types — compliant.
- `isSameFile` comparing normalized real paths is acceptable (S3 has no links).
- `setAttribute` throwing `UnsupportedOperationException` is acceptable (immutable S3 object
  metadata).

---

## Channel contracts

### 14. `FileChannel.read(ByteBuffer, long)` mutates the channel position — High

**File:** `S3FileChannel.java:453-460`

```java
public int read(ByteBuffer dst, long position) throws IOException {
    ...
    byteChannel.position(position);   // never restored
    return byteChannel.read(dst);
}
```

The contract states this method *does not modify the channel's position*. The sibling
`write(src, position)` correctly saves and restores the position; the read path does not.

**Fix (non-breaking bug fix):** Save the current position, seek, read, then restore — mirror
the `write(src, position)` implementation. Add a test asserting the position is unchanged
after a positional read.

### 15. `AsynchronousFileChannel` positional ops race on shared position — High

**File:** `AsyncS3FileChannel.java:75-88` (read), `101-114` (write)

Each positional op does a non-atomic `byteChannel.position(p)` then `read`/`write` on the
single shared `S3SeekableByteChannel`, dispatched on the common ForkJoinPool. The
`AsynchronousFileChannel` contract explicitly supports *multiple concurrent read and write
operations*; here two concurrent positional reads at different offsets can interleave and
fill buffers from the wrong offset.

**Fix (non-breaking bug fix):** Guard the position+IO pair with `synchronized(byteChannel)`
(as `S3FileChannel` does for `transferTo`/positional `write`), or give each operation its own
positioned view/read request. Add a concurrency test.

### 16. `createTempFile()` check-then-act race → spurious `FileAlreadyExistsException` — High — [#771]

**File:** `S3FileSystem.java:536-555`

```java
newPath = Files.exists(newPath)
    ? temporaryDirectory.resolve(filename + "-" + System.nanoTime())
    : newPath;
return Files.createFile(newPath);   // TOCTOU
```

Two threads opening write channels to the same S3 key compute the same local temp path, both
observe `Files.exists() == false`, and both call `Files.createFile(...)`; the loser gets a
`FileAlreadyExistsException` from the *local* temp file, unrelated to the `CREATE_NEW` open
option. This is the reported bug.

**Fix (non-breaking bug fix):** Create a guaranteed-unique temp file, e.g.
`Files.createTempFile(parent, filename, ".tmp")`, or catch the collision and retry with a
fresh unique name. Do not gate creation on a prior `Files.exists` check.

### 17. `truncate()` always throws `UnsupportedOperationException` — Medium — Breaking

**Files:** `S3SeekableByteChannel.java:282-284`, `S3WritableByteChannel.java:126-128`
(propagated via `S3FileChannel` and `AsyncS3FileChannel`)

`SeekableByteChannel.truncate` / `FileChannel.truncate` declare only
`NonWritableChannelException`, `ClosedChannelException`, `IllegalArgumentException`, and
`IOException` — not `UnsupportedOperationException`. The write channel is backed by a real
local temp-file `SeekableByteChannel` that supports truncation, so the refusal is artificial.
(Note: `TRUNCATE_EXISTING` as an *open option* is honored; only the explicit `truncate()`
call is not.)

**Fix (breaking):** Implement `truncate()` on writable channels by delegating to the local
temp-file channel; throw `NonWritableChannelException` for read-only channels.

### 18. `DELETE_ON_CLOSE` is ignored for the S3 object — Medium

**File:** `S3WritableByteChannel.java` (close path around `79-90`, option passed at `56-58`)

With `DELETE_ON_CLOSE`, the entity should not persist after `close()`. Here the option is
applied to the *local temp file* (which is deleted anyway), while `close()` still uploads the
object to S3, so the S3 key is created and left behind.

**Fix (non-breaking bug fix):** When `DELETE_ON_CLOSE` is present, skip the upload on close
(or delete the object immediately after upload), so no residual object remains.

### 22. `validateOpen()` reads `closed` without synchronization — Low

**File:** `S3SeekableByteChannel.java:327-331` reads `closed` outside a lock, whereas
`close()` and `isOpen()` synchronize on `this`. A `read`/`write` on one thread may not observe
a `close()` on another, defeating the intended `ClosedChannelException`.

**Fix (non-breaking bug fix):** Make `closed` `volatile` or read it under the same lock.

### Channels — not violations (reviewed and confirmed correct)

- `CREATE_NEW` correctly throws `FileAlreadyExistsException` (non-streaming and streaming
  paths).
- Reading a non-existent object throws `NoSuchFileException`.
- Double-close no longer re-uploads (issue [#453] fixed): the re-upload is gated on the
  `open` flag.
- Incomplete buffer fills across fragment boundaries are fixed (issue [#761]); the read loop
  fills the destination buffer and has a regression test.
- `position(newPosition)` past end is stored verbatim; a subsequent read past `size` returns
  `-1`, matching the contract.

---

## File-system lifecycle

### 21. `closeFileSystem()` is not thread-safe — Medium — [#773]

**File:** `S3FileSystemProvider.java:886-908`

```java
for (var key : FS_CACHE.keySet()) {
    if (fs == FS_CACHE.get(key)) {
        try (FileSystem closeable = FS_CACHE.remove(key)) {   // check-then-act
            closeFileSystemIfOpen(closeable);
            return;
        }
        ...
    }
}
```

Two threads can both pass the identity check; the first `remove(key)` succeeds, the second
returns `null`, which is then dereferenced in `closeFileSystemIfOpen(null)` →
`NullPointerException`. The try-with-resources is also redundant here because
`closeFileSystem()` is invoked *from* `S3FileSystem.close()`, so it re-enters close.

**Fix (non-breaking bug fix):** Use atomic conditional removal and avoid recursive close:

```java
void closeFileSystem(FileSystem fs) {
    for (var key : FS_CACHE.keySet()) {
        if (FS_CACHE.remove(key, fs)) {   // atomic; true only for the owning thread
            return;
        }
    }
}
```

The owning `S3FileSystem.close()` already closes channels and flips `open`, so
`closeFileSystem` only needs to evict the cache entry. Make the eviction idempotent. This also
relates to the `FS_CACHE` design changes in
[`newfilesystem-contract-fix.md`](./newfilesystem-contract-fix.md).

---

## Cross-cutting recommendations

1. **Separate abstract-path operations from I/O and normalization.** Findings #2–#6 share a
   root cause: `S3Path` conflates the *syntactic* path with its *normalized real* form, and
   `subpath()`/`getName()` carry S3 "directory" trailing separators into places the `Path`
   contract expects bare name elements. A focused refactor of `PosixLikePathRepresentation`
   and the `S3Path` name/element accessors — bare elements internally, absoluteness tracked as
   a flag, separators applied only at `toString()` — resolves the cluster coherently.

2. **Distinguish "S3 has no real directories" deviations from bugs.** Where the divergence is
   intentional (recursive `copy`, syntactic `isDirectory()`), document it as prominently as
   the existing `copy` note. Where it silently inverts a contract (`delete`, `createDirectory`,
   `newDirectoryStream`), fix it.

3. **Ship the contract-affecting changes as a single major release.** Findings #2–#6, #8–#13,
   #17, and #20 change observable behavior. Group them, call them out in `RELEASE.md`, and
   provide a migration note (especially for `equals`/`hashCode` and `delete` semantics).

4. **Add contract-conformance tests.** Consider running (a subset of) the JDK's own
   `java.nio.file` TCK-style tests, or port the key invariants (parent chain reaches root,
   `relativize`/`resolve` round-trip, iterator yields bare elements, `delete` throws on
   missing/non-empty) into the existing test suite so regressions are caught.

## Suggested fix ordering

1. **#1** (`char[]` constructor) — trivial, unblocks correct `relativize`, no compatibility
   impact.
2. **#16, #21, #14, #15, #18, #22** — thread-safety / correctness bug fixes, non-breaking;
   resolve open issues [#771] and [#773].
3. **#2, #7** — `getParent`/`resolveSibling`; resolve [#772] and unblock
   `Files.createDirectories`.
4. **#3, #4, #5, #6** — the `S3Path` abstract-path refactor (breaking; major release).
5. **#8, #9, #10, #11, #12, #13, #17, #20** — provider/channel contract corrections (breaking;
   major release).
6. **#19, #23** — `checkAccess` policy decision and javadoc cleanup.

[#770]: https://github.com/awslabs/aws-java-nio-spi-for-s3/pull/770
[#771]: https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/771
[#772]: https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/772
[#773]: https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/773
[#453]: https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/453
[#761]: https://github.com/awslabs/aws-java-nio-spi-for-s3/issues/761
