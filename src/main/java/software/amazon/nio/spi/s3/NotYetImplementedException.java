package software.amazon.nio.spi.s3;

public class NotYetImplementedException extends RuntimeException {
    public NotYetImplementedException(String message) {
        super(message);
    }
}
