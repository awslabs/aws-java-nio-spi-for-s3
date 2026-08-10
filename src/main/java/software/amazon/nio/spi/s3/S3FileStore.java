/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.FileStore;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Objects;

/**
 * A {@link FileStore} representing an S3 bucket.
 * <br>
 * S3 buckets have no partitions or volumes and no meaningful notion of total, usable, or
 * unallocated space, so the space-reporting methods return {@link Long#MAX_VALUE}. This class
 * exists so that {@link java.nio.file.Files#getFileStore(java.nio.file.Path)} and
 * {@link java.nio.file.FileSystem#getFileStores()} return a usable, non-null value as required by
 * their contracts, rather than {@code null}.
 */
class S3FileStore extends FileStore {

    private static final String TYPE = "s3";

    private final String bucketName;

    S3FileStore(String bucketName) {
        this.bucketName = Objects.requireNonNull(bucketName, "bucketName may not be null");
    }

    /**
     * @return the name of the bucket represented by this file store
     */
    @Override
    public String name() {
        return bucketName;
    }

    /**
     * @return the constant string {@code "s3"}
     */
    @Override
    public String type() {
        return TYPE;
    }

    /**
     * S3 itself is not inherently read-only; whether writes succeed depends on the caller's IAM
     * permissions and the bucket policy.
     *
     * @return {@code false}
     */
    @Override
    public boolean isReadOnly() {
        return false;
    }

    /**
     * S3 imposes no practical limit on the total size of a bucket.
     *
     * @return {@link Long#MAX_VALUE}
     */
    @Override
    public long getTotalSpace() {
        return Long.MAX_VALUE;
    }

    /**
     * S3 imposes no practical limit on the space usable within a bucket.
     *
     * @return {@link Long#MAX_VALUE}
     */
    @Override
    public long getUsableSpace() {
        return Long.MAX_VALUE;
    }

    /**
     * S3 imposes no practical limit on the space usable within a bucket.
     *
     * @return {@link Long#MAX_VALUE}
     */
    @Override
    public long getUnallocatedSpace() {
        return Long.MAX_VALUE;
    }

    /**
     * This file store supports the {@code basic} file attribute view only, matching
     * {@link S3FileSystem#supportedFileAttributeViews()}.
     */
    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return type == BasicFileAttributeView.class;
    }

    /**
     * This file store supports the {@code basic} file attribute view only, matching
     * {@link S3FileSystem#supportedFileAttributeViews()}.
     */
    @Override
    public boolean supportsFileAttributeView(String name) {
        return S3FileSystem.BASIC_FILE_ATTRIBUTE_VIEW.equals(name);
    }

    /**
     * S3 file stores expose no {@link FileStoreAttributeView}s.
     *
     * @return {@code null} always
     */
    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        return null;
    }

    /**
     * S3 file stores expose no attributes.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Object getAttribute(String attribute) {
        throw new UnsupportedOperationException("S3 file stores do not support attribute '" + attribute + "'");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return bucketName.equals(((S3FileStore) o).bucketName);
    }

    @Override
    public int hashCode() {
        return bucketName.hashCode();
    }

    @Override
    public String toString() {
        return TYPE + "://" + bucketName;
    }
}
