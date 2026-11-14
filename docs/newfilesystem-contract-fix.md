# Fixing `newFileSystem` / `getFileSystem` Contract Compliance

> **Status:** Implemented in `3.0.0-rc1`, then **revised** during the configuration redesign
> (issue #601). `newFileSystem` treats `BucketAlreadyOwnedByYouException` as reuse and maps
> `BucketAlreadyExistsException` to `IOException`; `getFileSystem` throws
> `FileSystemNotFoundException` on a miss; `getPath` / `Paths.get(URI)` materialize a lazy,
> side-effect-free view; and `close()` removes the cache entry.
>
> **Revision (supersedes "Option A" below):** the separate `EXPLICITLY_CREATED` registry has been
> removed. `newFileSystem` now throws `FileSystemAlreadyExistsException` when a file system for the
> URI already exists in `FS_CACHE` — whether it was created by a previous `newFileSystem` call or
> lazily materialized by `getPath` / `Paths.get`. This is simpler and more faithful to the
> `java.nio.file.spi.FileSystemProvider` contract ("already exists" is about the in-JVM file-system
> instance, not how it came to exist). Regression coverage is in `S3FileSystemProviderTest`
> (`newFileSystemThrowsWhenViewAlreadyExists`) and `NioContractComplianceTest`.

## Background

PR [#770](https://github.com/awslabs/aws-java-nio-spi-for-s3/pull/770) reports that
`S3FileSystemProvider.newFileSystem(URI, Map)` throws an `IOException` on every call after the
first, because the underlying S3 bucket already exists. The author's use case is legitimate:
call `newFileSystem(s3://myBucket)` on each run of an application and reuse a persistent bucket.

This document analyses the `newFileSystem` / `getFileSystem` contract, compares the current
behavior against the JDK reference implementations, explains why the PR's patch is insufficient,
and specifies the recommended changes.

## The NIO contract

From `java.nio.file.spi.FileSystemProvider`:

- **`newFileSystem(URI, Map)`** — *"Throws `FileSystemAlreadyExistsException` if the file system
  already exists because it was previously created by an invocation of this method. Once a file
  system is closed it is provider-dependent if the provider allows a new file system to be created
  with the same URI as a file system it previously created."*
- **`getFileSystem(URI)`** — returns a filesystem *previously created by* `newFileSystem`, and
  throws `FileSystemNotFoundException` if it does not exist.

The decisive phrase is **"previously created by an invocation of this method."** The
`FileSystemAlreadyExistsException` decision must be based on whether a `FileSystem` *object* has
already been created **in this JVM**, not on whether some backing resource exists remotely.

### Reference implementation: `jdk.nio.zipfs.ZipFileSystemProvider`

```java
public FileSystem newFileSystem(URI uri, Map env) throws IOException {
    Path path = uriToPath(uri);
    synchronized (filesystems) {
        Path realPath = null;
        if (ensureFile(path)) {
            realPath = path.toRealPath();
            if (filesystems.containsKey(realPath))      // in-JVM tracking map
                throw new FileSystemAlreadyExistsException();
        }
        ZipFileSystem zipfs = getZipFileSystem(path, env);
        if (realPath == null) realPath = path.toRealPath();
        filesystems.put(realPath, zipfs);
        return zipfs;
    }
}

public FileSystem getFileSystem(URI uri) {
    synchronized (filesystems) {
        ZipFileSystem zipfs = filesystems.get(uriToPath(uri).toRealPath());
        if (zipfs == null) throw new FileSystemNotFoundException();
        return zipfs;
    }
}

void removeFileSystem(Path zfpath, ZipFileSystem zfs) throws IOException {
    synchronized (filesystems) { filesystems.remove(zfpath.toRealPath(), zfs); }
}
```

Observations:
- `FileSystemAlreadyExistsException` is thrown **only** when a filesystem object is already in the
  `filesystems` map. The existence of the backing zip file on disk does not, by itself, trigger it.
- `getFileSystem` **reads only** — it never lazily creates — and throws `FileSystemNotFoundException`
  on a miss. Only `newFileSystem` populates the map.
- `removeFileSystem` (on close) removes the entry, allowing re-creation with the same URI later.

### Reference implementation: `sun.nio.fs.UnixFileSystemProvider` (default provider)

```java
public final FileSystem newFileSystem(URI uri, Map env) {
    checkUri(uri);
    throw new FileSystemAlreadyExistsException();   // the single default FS exists at startup
}
public final FileSystem getFileSystem(URI uri) {
    checkUri(uri);
    return theFileSystem;
}
```

Again, the exception reflects the lifecycle of the in-JVM `FileSystem` instance, never a backing
store.

## Current behavior in this library

`S3FileSystemProvider.newFileSystem(URI, Map)`:

1. Validates the scheme.
2. Unconditionally calls the S3 `createBucket` API — a remote, persistent side effect.
3. Translates `BucketAlreadyOwnedByYouException` **and** `BucketAlreadyExistsException` into
   `FileSystemAlreadyExistsException`.
4. Only on successful bucket creation adds the `S3FileSystem` to `FS_CACHE` via
   `getOrCreateFileSystem` (`computeIfAbsent`).

`S3FileSystemProvider.getFileSystem(URI)` is implemented as `getOrCreateFileSystem`
(`computeIfAbsent`) — it **lazily creates** and never throws `FileSystemNotFoundException`.
`getPath(URI)` calls `getFileSystem`, so constructing a `Path` also populates `FS_CACHE`.

### Problems

1. **Wrong trigger for `FileSystemAlreadyExistsException`.** The decision is keyed on remote bucket
   state, not on `FS_CACHE`. `FS_CACHE` is never consulted before the exception is thrown. A bucket
   that persists between runs (or is owned by you but created elsewhere) makes the *first*
   `newFileSystem` call in a fresh JVM throw — the reported bug.
2. **Heavy unconditional remote side effect.** Every `newFileSystem` call provisions a bucket,
   conflating "instantiate a `FileSystem` view" with "provision the backing store."
3. **`BucketAlreadyExistsException` is mis-mapped.** A bucket owned by *another account* is an
   access/ownership failure, not "the file system you created already exists."
4. **`getFileSystem` is non-conformant.** It lazily creates instead of throwing
   `FileSystemNotFoundException`, so callers cannot distinguish "created" from "not created."

## Why the PR's patch is insufficient

The PR changes the `catch` block so that `BucketAlreadyOwnedByYouException` is swallowed and control
falls through to `getOrCreateFileSystem`:

```java
} catch (ExecutionException e) {
    if (e.getCause() instanceof BucketAlreadyExistsException) {
        throw (FileSystemAlreadyExistsException) new FileSystemAlreadyExistsException(...)...;
    } else if (!(e.getCause() instanceof BucketAlreadyOwnedByYouException)) {
        throw new IOException(e.getMessage(), e.getCause());
    }
}
```

This fixes the reported symptom (reusing an owned bucket across JVM runs) but:

- **Still keys the decision on bucket state, not `FS_CACHE`.** A second `newFileSystem` for the same
  bucket in the *same JVM* should throw `FileSystemAlreadyExistsException`, but now it silently
  returns the cached instance (`computeIfAbsent` finds it and returns it). This is a **new contract
  violation** — worse than before, where the duplicate call at least threw.
- Leaves `BucketAlreadyExistsException` mis-mapped and `getFileSystem` non-conformant.

## The deeper coupling: lazy creation poisons the "already exists" gate

`getPath` → `getFileSystem` → `getOrCreateFileSystem` writes to the **same** `FS_CACHE` that a
corrected `newFileSystem` must treat as authoritative. If `newFileSystem` is fixed to gate on
`FS_CACHE` but lazy creation still writes to it:

```java
Path p = Paths.get(URI.create("s3://myBucket/key")); // populates FS_CACHE for myBucket
provider.newFileSystem(URI.create("s3://myBucket"), env); // would throw FileSystemAlreadyExistsException
```

`newFileSystem` throws even though it was never previously called and the bucket may not exist —
and the bucket is never provisioned. In `ZipFileSystemProvider` this cannot happen because
`getFileSystem` never creates; only `newFileSystem` populates the tracking map.

**Conclusion:** the fix must separate *filesystems explicitly created by `newFileSystem`* from
*filesystem views lazily materialized by `getPath`*, rather than gating the former on a cache that
the latter also populates.

## Recommended changes

### Change 1 — Track explicit creations, gate `newFileSystem` on the JVM instance cache

Base `FileSystemAlreadyExistsException` on `FS_CACHE` (the in-JVM instance registry), reserve the
slot before any remote side effect, treat `BucketAlreadyOwnedByYouException` as "backing store
already provisioned — proceed", and map `BucketAlreadyExistsException` to `IOException`.

```java
@Override
public FileSystem newFileSystem(final URI uri, final Map<String, ?> env) throws IOException {
    if (!uri.getScheme().equals(getScheme())) {
        throw new IllegalArgumentException("URI scheme must be " + getScheme());
    }

    @SuppressWarnings("unchecked")
    var envMap = (env != null) ? (Map<String, Object>) env : Collections.<String, Object>emptyMap();

    var info = fileSystemInfo(uri);
    var config = new S3NioSpiConfiguration().withEndpoint(info.endpoint()).withBucketName(info.bucket());
    if (info.accessKey() != null) {
        config.withCredentials(info.accessKey(), info.accessSecret());
    }

    // Contract: FileSystemAlreadyExistsException reflects whether a FileSystem for this URI was
    // previously created (in this JVM) by an invocation of this method -- NOT whether the backing
    // S3 bucket exists. Reserve the cache slot up front so a duplicate call fails fast, before any
    // remote side effect (cf. ZipFileSystemProvider).
    var newFs = new S3FileSystem(this, config);
    if (FS_CACHE.putIfAbsent(info.key(), newFs) != null) {
        closeQuietly(newFs);
        throw new FileSystemAlreadyExistsException(
                "a file system for '" + info.key() + "' already exists");
    }

    var bucketName = config.getBucketName();
    try (var client = new S3ClientProvider(config).configureCrtClient().build()) {
        var createBucketResponse = client.createBucket(
                bucketBuilder -> bucketBuilder.bucket(bucketName)
                        .acl(envMap.getOrDefault("acl", "").toString())
                        .grantFullControl(envMap.getOrDefault("grantFullControl", "").toString())
                        .grantRead(envMap.getOrDefault("grantRead", "").toString())
                        .grantReadACP(envMap.getOrDefault("grantReadACP", "").toString())
                        .grantWrite(envMap.getOrDefault("grantWrite", "").toString())
                        .grantWriteACP(envMap.getOrDefault("grantWriteACP", "").toString())
                        .createBucketConfiguration(confBuilder -> {
                            if (envMap.containsKey("locationConstraint")) {
                                String loc = envMap.get("locationConstraint").toString();
                                if (loc.equals(Region.US_EAST_1.id())) {
                                    loc = null; // us-east-1 is the default (null) location for S3
                                }
                                confBuilder.locationConstraint(loc);
                            }
                        })
        ).get(30, TimeUnit.SECONDS);
        logger.debug("Create bucket response {}", createBucketResponse.toString());

    } catch (ExecutionException e) {
        var cause = e.getCause();
        if (cause instanceof BucketAlreadyOwnedByYouException) {
            // Backing store already provisioned and owned by us: this is analogous to opening an
            // existing backing file. Proceed with the reserved, cached FileSystem.
            logger.debug("Bucket '{}' already exists and is owned by you; reusing it", bucketName);
        } else if (cause instanceof BucketAlreadyExistsException) {
            // Owned by another account -- an access/ownership failure, not "FS already exists".
            FS_CACHE.remove(info.key(), newFs);
            closeQuietly(newFs);
            throw new IOException("bucket '" + bucketName
                    + "' already exists and is owned by another account", cause);
        } else {
            FS_CACHE.remove(info.key(), newFs);
            closeQuietly(newFs);
            throw new IOException(e.getMessage(), cause);
        }
    } catch (InterruptedException | TimeoutException | SdkException e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        FS_CACHE.remove(info.key(), newFs);
        closeQuietly(newFs);
        throw new IOException(e.getMessage(), e);
    }

    return newFs;
}

private void closeQuietly(S3FileSystem fs) {
    try {
        fs.close();
    } catch (IOException | RuntimeException ignore) {
        logger.debug("failed to close redundant file system instance", ignore);
    }
}
```

### Change 2 — Make `getFileSystem` contract-compliant, keep `getPath` ergonomic

`getFileSystem` should throw `FileSystemNotFoundException` on a miss. The library's headline
`Paths.get(URI.create("s3://..."))` ergonomics are preserved by routing `getPath` through the
lazy, side-effect-free creator instead of `getFileSystem`.

```java
@Override
public FileSystem getFileSystem(URI uri) {
    var info = fileSystemInfo(uri);
    var fs = FS_CACHE.get(info.key());
    if (fs == null) {
        throw new FileSystemNotFoundException("no file system for '" + info.key()
                + "'; call newFileSystem(URI, env) first");
    }
    return fs;
}

@Override
public Path getPath(URI uri) {
    Objects.requireNonNull(uri);
    var info = fileSystemInfo(uri);
    var config = new S3NioSpiConfiguration().withEndpoint(info.endpoint()).withBucketName(info.bucket());
    if (info.accessKey() != null) {
        config.withCredentials(info.accessKey(), info.accessSecret());
    }
    var fs = getOrCreateFileSystem(info.key(), config); // lazy view; does NOT provision a bucket
    return fs.getPath(uri.getScheme() + ":/" + uri.getPath());
}
```

### Change 3 — Decouple lazy creation from the `newFileSystem` "already exists" gate

Changes 1 and 2 alone do not stop `getPath`'s lazy creation from populating the cache that
`newFileSystem` gates on. Choose one of the following:

- **Option A (recommended): separate registries.** Keep a dedicated set of URIs/keys explicitly
  created via `newFileSystem` and base `FileSystemAlreadyExistsException` on that set. Lazy views
  created by `getPath` live in the instance cache but do not count as "created by `newFileSystem`".
  This most faithfully matches the reference model while preserving lazy `Paths.get`.

  Sketch:

  ```java
  private static final Map<String, S3FileSystem> FS_CACHE = new ConcurrentHashMap<>();
  private static final Set<String> EXPLICITLY_CREATED = ConcurrentHashMap.newKeySet();

  // in newFileSystem, gate and reserve on EXPLICITLY_CREATED:
  if (!EXPLICITLY_CREATED.add(info.key())) {
      throw new FileSystemAlreadyExistsException("a file system for '" + info.key() + "' already exists");
  }
  // ... provision bucket; on failure: EXPLICITLY_CREATED.remove(info.key()); ...
  // register/return the instance from FS_CACHE (computeIfAbsent or putIfAbsent)

  // in closeFileSystem, also remove from EXPLICITLY_CREATED so re-creation is allowed after close.
  ```

- **Option B: keep a single cache and accept the coupling.** Ship Changes 1 + 2 only. Document that
  touching a `Path` for a bucket before `newFileSystem` will cause `newFileSystem` to throw
  `FileSystemAlreadyExistsException` for that bucket in the same JVM. Simpler, but leaves the
  surprising order-dependent behavior described above.

### Change 4 — Ensure `close()` clears all registries

`closeFileSystem` already removes the entry from `FS_CACHE`. If Option A is adopted, it must also
remove the key from `EXPLICITLY_CREATED`, so that after a filesystem is closed a new one can be
created for the same URI (permitted and provider-dependent per the contract).

## Behavior matrix (target)

| Scenario | Current | PR #770 | Recommended |
|---|---|---|---|
| First `newFileSystem`, bucket does not exist | creates bucket + FS | same | creates bucket + FS |
| First `newFileSystem`, bucket exists & owned by you | throws `IOException`/`FSAEE` | returns FS | returns FS (reuse) |
| Second `newFileSystem`, same JVM, same bucket | throws `FSAEE` (wrong reason) | **returns silently (wrong)** | throws `FileSystemAlreadyExistsException` |
| `newFileSystem`, bucket owned by another account | `FileSystemAlreadyExistsException` | `FileSystemAlreadyExistsException` | `IOException` (access/ownership) |
| `newFileSystem` after `close()`, same URI | works | works | works |
| `getFileSystem` for never-created FS | lazily creates | lazily creates | throws `FileSystemNotFoundException` |
| `Paths.get(URI)` without prior `newFileSystem` | works (lazy) | works (lazy) | works (lazy, no bucket provisioning) |

*FSAEE = `FileSystemAlreadyExistsException`.*

## Suggested tests

Unit tests (mock the S3 client / `createBucket`):

1. First `newFileSystem` succeeds when the bucket is pre-existing and owned by you
   (`BucketAlreadyOwnedByYouException` is treated as success).
2. Second `newFileSystem` for the same bucket in the same JVM throws
   `FileSystemAlreadyExistsException`, and does **not** issue a second `createBucket` call.
3. `newFileSystem` for a bucket owned by another account (`BucketAlreadyExistsException`) throws
   `IOException`, and the reserved cache slot is rolled back.
4. `newFileSystem` after `close()` for the same URI succeeds (registries cleared on close).
5. `getFileSystem` for a never-created filesystem throws `FileSystemNotFoundException`.
6. `Paths.get(URI.create("s3://bucket/key"))` works without a prior `newFileSystem` and does not
   provision a bucket.
7. (Option A) `getPath`/`Paths.get` before `newFileSystem` does **not** cause `newFileSystem` to
   throw `FileSystemAlreadyExistsException`.

## Compatibility notes

- Making `getFileSystem` throw `FileSystemNotFoundException` is a **behavioral change** for callers
  that currently rely on `provider.getFileSystem(uri)` to lazily create. The `getPath` rewrite keeps
  the primary `Paths.get(URI)` use case working, so end users constructing paths are unaffected.
- If a smaller, lower-risk PR is preferred, Change 1 alone resolves the reported bug and the
  `newFileSystem` contract for the common "`newFileSystem` first" flow, at the cost of the coupling
  described in Change 3. Changes 2–4 should follow to make the provider fully conformant.
