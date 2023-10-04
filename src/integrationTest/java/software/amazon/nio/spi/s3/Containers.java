package software.amazon.nio.spi.s3;

import org.testcontainers.containers.Container;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.S3_SPI_ENDPOINT_PROTOCOL_PROPERTY;

abstract class Containers {

    static final LocalStackContainer LOCAL_STACK_CONTAINER;

    static {
        LOCAL_STACK_CONTAINER = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:2.3.2")
        ).withServices(S3);
        LOCAL_STACK_CONTAINER.start();
        System.setProperty(S3_SPI_ENDPOINT_PROTOCOL_PROPERTY, "http");
    }

    public static void createBucket(String name) {
        AtomicReference<Container.ExecResult> execResult = new AtomicReference<>();
        assertThatCode(() ->
            execResult.set(LOCAL_STACK_CONTAINER.execInContainer(("awslocal s3api create-bucket --bucket " + name).split(" ")))
        ).doesNotThrowAnyException();
        assertThat(execResult.get().getExitCode()).isZero();
    }

    public static void putObject(String bucket, String key) {
        AtomicReference<Container.ExecResult> execResult = new AtomicReference<>();
        assertThatCode(() ->
            execResult.set(LOCAL_STACK_CONTAINER.execInContainer(("awslocal s3api put-object --bucket "+bucket+" --key "+key).split(" ")))
        ).doesNotThrowAnyException();
        assertThat(execResult.get().getExitCode()).isZero();
    }

    public static String localStackConnectionEndpoint() {
        return localStackConnectionEndpoint(null, null);
    }

    public static String localStackConnectionEndpoint(String key, String secret) {
        String accessKey = key != null ? key : LOCAL_STACK_CONTAINER.getAccessKey();
        String secretKey = secret != null ? secret : LOCAL_STACK_CONTAINER.getSecretKey();
        return String.format("s3x://%s:%s@%s", accessKey, secretKey, localStackHost());
    }

    private static String localStackHost() {
        return LOCAL_STACK_CONTAINER.getEndpoint().getHost() + ":" + LOCAL_STACK_CONTAINER.getEndpoint().getPort();
    }
}
