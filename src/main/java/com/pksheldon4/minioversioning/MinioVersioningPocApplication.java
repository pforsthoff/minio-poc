package com.pksheldon4.minioversioning;

import com.pksheldon4.minioversioning.config.MinioProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(MinioProperties.class)
public class MinioVersioningPocApplication {

    public static void main(String[] args) {
        SpringApplication.run(MinioVersioningPocApplication.class, args);
    }

}
