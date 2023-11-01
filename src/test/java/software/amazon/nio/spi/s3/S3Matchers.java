package software.amazon.nio.spi.s3;

import org.mockito.ArgumentMatchers;

import java.util.function.Consumer;

public class S3Matchers {
    public static <T> Consumer<T> anyConsumer() {
        return ArgumentMatchers.any();
    }
}
