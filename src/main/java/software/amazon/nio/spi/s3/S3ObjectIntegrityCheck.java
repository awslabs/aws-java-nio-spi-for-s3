/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.Checksum;
import software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder;
import software.amazon.awssdk.utils.BinaryUtils;

/**
 * Defines how to create a checksum to check the integrity of an object uploaded to S3.
 */
public abstract class S3ObjectIntegrityCheck extends S3OpenOption {
    private final byte[] buffer;
    private final Checksum checksum;

    protected S3ObjectIntegrityCheck(
        byte[] buffer,
        Checksum checksum) {
        this.buffer = buffer;
        this.checksum = checksum;
    }

    /**
     * Translates the given 32-bit checksum number to an Base64-encoded string.
     *
     * @param checksum
     *            32-bit checksum number
     * @return checksum as Base64-encoded string
     */
    public static String checksumToBase64String(int checksum) {
        return BinaryUtils.toBase64(checksumToByteArray(checksum));
    }

    /**
     * Translates the given 64-bit checksum number to an Base64-encoded string.
     *
     * @param checksum
     *            64-bit checksum number
     * @return checksum as Base64-encoded string
     */
    public static String checksumToBase64String(long checksum) {
        return BinaryUtils.toBase64(checksumToByteArray(checksum));
    }

    /**
     * Translates the given 32-bit checksum number to a byte array.
     *
     * @param checksum
     *            32-bit checksum number
     * @return checksum as byte array
     */
    public static byte[] checksumToByteArray(int checksum) {
        return new byte[] {
                (byte) (checksum >>> 24),
                (byte) (checksum >>> 16),
                (byte) (checksum >>> 8),
                (byte) checksum,
        };
    }

    /**
     * Translates the given 64-bit checksum number to a byte array.
     *
     * @param checksum
     *            64-bit checksum number
     * @return checksum as byte array
     */
    public static byte[] checksumToByteArray(long checksum) {
        return new byte[] {
                (byte) (checksum >>> 56),
                (byte) (checksum >>> 48),
                (byte) (checksum >>> 40),
                (byte) (checksum >>> 32),
                (byte) (checksum >>> 24),
                (byte) (checksum >>> 16),
                (byte) (checksum >>> 8),
                (byte) checksum,
        };
    }

    /**
     * Calculates the checksum for the specified file and adds it as a header to the PUT object request to be created.
     *
     * @param putObjectRequest
     *            put object request
     * @param file
     *            the local file to be used for creating the checksum
     */
    @Override
    protected abstract void apply(Builder putObjectRequest, Path file);

    /**
     * Calculates the checksum for the given file.
     *
     * @param file
     * @return checksum
     */
    protected final long calculateChecksum(Path file) {
        checksum.reset();
        try (var in = Files.newInputStream(file)) {
            int len;
            while ((len = in.read(buffer)) != -1) {
                checksum.update(buffer, 0, len);
            }
            return checksum.getValue();
        } catch (IOException cause) {
            throw new UncheckedIOException(cause);
        }
    }
}
