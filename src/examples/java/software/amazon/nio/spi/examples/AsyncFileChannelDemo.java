/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.nio.spi.examples;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

public class AsyncFileChannelDemo {
    public static void main(String[] args) {
        var path = Paths.get(URI.create(args[0]));
        try (var channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ)) {
            var buffer = ByteBuffer.allocate(1024);
            long position = 0;
            int bytesRead;
            while ((bytesRead = channel.read(buffer, position).get(10, TimeUnit.SECONDS)) != -1) {
                position += bytesRead;
                buffer.flip();
                while (buffer.hasRemaining()) {
                    System.out.print((char) buffer.get());
                }
                buffer.clear();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
