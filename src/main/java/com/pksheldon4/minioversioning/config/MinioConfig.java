package com.pksheldon4.minioversioning.config;

import io.minio.BucketExistsArgs;
import io.minio.GetBucketVersioningArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.SetBucketVersioningArgs;
import io.minio.messages.VersioningConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static io.minio.messages.VersioningConfiguration.Status.ENABLED;

@Configuration
public class MinioConfig {

    MinioProperties properties;

    public MinioConfig(MinioProperties minioProperties) {
        this.properties = minioProperties;
    }

    @Bean
    public MinioClient minioClient() throws Exception {
        final String bucketName = properties.getBucket();
        MinioClient minioClient = MinioClient.builder()
            .endpoint(properties.getEndpoint())
            .credentials(properties.getAccessKey(), properties.getSecretKey())
            .build();
        if (minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            if (!minioClient.getBucketVersioning(GetBucketVersioningArgs.builder().bucket(bucketName).build()).status().equals(ENABLED)) {
                minioClient.setBucketVersioning(SetBucketVersioningArgs.builder().bucket(bucketName).config(versioningConfiguration()).build());
            }
        } else {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            minioClient.setBucketVersioning(SetBucketVersioningArgs.builder().bucket(bucketName).config(versioningConfiguration()).build());
        }
        return minioClient;
    }

    private VersioningConfiguration versioningConfiguration() {
        return new VersioningConfiguration(ENABLED, true); //Not sure how mfaDelete is used
    }
}
