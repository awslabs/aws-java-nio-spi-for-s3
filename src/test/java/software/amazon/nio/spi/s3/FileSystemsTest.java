/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.s3;

import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class FileSystemsTest {

    @ParameterizedTest
    @MethodSource("uris")
    @DisplayName("newFileSystem(URI, env) should throw")
    public void newFileSystemURI(URI uri) {
//        assertThatThrownBy(
//            () -> FileSystems.newFileSystem(uri, Collections.emptyMap())
//        ).isInstanceOf(NotYetImplementedException.class);
    }

    private static Stream<Arguments> uris() {
        return Stream.of(
            Arguments.of(URI.create("s3://foo/baa")),
            Arguments.of(URI.create("s3x://foo/baa"))
        );
    }
}
