package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("S3TransferUtil")
class S3TransferUtilTest {

    @Test
    @DisplayName("upload should succeed")
    void uploadFileCompletesSuccessfully() throws IOException {
        S3Path file = mock();
        when(file.bucketName()).thenReturn("a");
        when(file.getKey()).thenReturn("a");

        final S3AsyncClient client = mock();
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(completedFuture(PutObjectResponse.builder().build()));

        S3TransferUtil util = new S3TransferUtil(client, 1L, TimeUnit.MINUTES);
        Path tmpFile = Files.createTempFile(null, null);
        assertThatCode(() -> util.uploadLocalFile(file, tmpFile)).doesNotThrowAnyException();
    }
}