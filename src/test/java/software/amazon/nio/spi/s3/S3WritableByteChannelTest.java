package software.amazon.nio.spi.s3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.FileAlreadyExistsException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3WritableByteChannelTest {

    @Test
    @DisplayName("when file exists and constructor is invoked with option `CREATE_NEW` should throw FileAlreadyExistsException")
    void whenFileExistsAndCreateNewShouldThrowFileAlreadyExistsException() throws InterruptedException, TimeoutException {
        S3FileSystemProvider provider = mock();
        when(provider.exists(any(), any())).thenReturn(true);

        S3FileSystem fs = mock();
        when(fs.provider()).thenReturn(provider);

        S3Path file = S3Path.getPath(fs, "somefile");
        assertThatThrownBy(() -> new S3WritableByteChannel(file, mock(), Set.of(CREATE_NEW), 0L, TimeUnit.MINUTES))
                .isInstanceOf(FileAlreadyExistsException.class);
    }
}