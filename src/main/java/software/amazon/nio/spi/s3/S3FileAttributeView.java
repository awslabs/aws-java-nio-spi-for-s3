/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class S3FileAttributeView implements BasicFileAttributeView {

    private final S3Path path;

    protected S3FileAttributeView(S3Path path){
        this.path = path;
    }

    /**
     * Returns the name of the attribute view. Attribute views of this type
     * have the name {@code "basic"}.
     */
    @Override
    public String name() {
        return "s3";
    }
    /**
     * Reads the basic file attributes as a bulk operation.
     *
     * <p> It is implementation specific if all file attributes are read as an
     * atomic operation with respect to other file system operations.
     *
     * @return the file attributes
     */
    @Override
    public BasicFileAttributes readAttributes() {
        return new S3BasicFileAttributes(path);
    }

    /**
     * Unsupported operation, write operations are not supported.
     */
    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) {
        throw new UnsupportedOperationException("write operations are not supported, please submitted a feature request explaining your use case");
    }

}
