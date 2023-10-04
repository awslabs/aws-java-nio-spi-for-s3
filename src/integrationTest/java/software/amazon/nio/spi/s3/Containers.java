package software.amazon.nio.spi.s3;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_REGION_PROPERTY;
import static software.amazon.nio.spi.s3.config.S3NioSpiConfiguration.AWS_SECRET_ACCESS_KEY_PROPERTY;

abstract class Containers {

    static final LocalStackContainer LOCAL_STACK_CONTAINER;

    static {
        LOCAL_STACK_CONTAINER = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:2.3.2")
        ).withServices(S3);
        LOCAL_STACK_CONTAINER.start();

        System.setProperty("aws.accessKeyId", LOCAL_STACK_CONTAINER.getAccessKey());
        System.setProperty(AWS_SECRET_ACCESS_KEY_PROPERTY, LOCAL_STACK_CONTAINER.getSecretKey());
        System.setProperty(AWS_REGION_PROPERTY, LOCAL_STACK_CONTAINER.getRegion());
    }

    public static String localStackHost() {
        return LOCAL_STACK_CONTAINER.getEndpoint().getHost() + ":" + LOCAL_STACK_CONTAINER.getEndpoint().getPort();
    }
}
