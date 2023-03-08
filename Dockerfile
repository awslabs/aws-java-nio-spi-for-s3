# Example dockerfile that generates an image using GATK and using this package as a provider for the 's3' scheme.
FROM us.gcr.io/broad-gatk/gatk:4.2.0.0
COPY build/libs/nio-spi-for-s3-1.2.0-all.jar /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/ext/nio-spi-for-s3-1.2.0-all.jar
WORKDIR /gatk
CMD ["bash", "--init-file", "/gatk/gatkenv.rc"]
ENV PATH /gatk:$PATH
