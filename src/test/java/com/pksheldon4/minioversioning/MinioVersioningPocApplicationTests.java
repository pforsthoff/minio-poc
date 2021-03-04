package com.pksheldon4.minioversioning;

import com.pksheldon4.minioversioning.config.MinioProperties;
import io.minio.GetBucketVersioningArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ActiveProfiles;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.minio.messages.VersioningConfiguration.Status.ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Slf4j
class MinioVersioningPocApplicationTests {

    private static final String TEST_FILE_NAME = "test-file.txt";

    @Autowired
    private MinioClient minioClient;

    @Autowired
    private MinioProperties minioProperties;

    @AfterEach
    public void cleanUp() throws Exception {
        List<Item> results = getItemsFromBucket(minioProperties.getBucket(), true);
        for (Item item : results) {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(minioProperties.getBucket()).object(item.objectName()).versionId(item.versionId()).build());
            log.info("Removing Object {}, Version {}, LastModified {}, deleteMarker {}, {} ", item.objectName(), item.versionId(), item.lastModified(), item.isDeleteMarker(), item.getClass());
        }
        results = getItemsFromBucket(minioProperties.getBucket(), true);
        assertThat(results.size()).isEqualTo(0);
    }


    @Test
    void bucketVersioningIsEnabled() throws Exception {
        assertThat(minioClient).isNotNull();
        boolean isVersioningEnabled =
            minioClient.getBucketVersioning(GetBucketVersioningArgs.builder().bucket(minioProperties.getBucket()).build()).status().equals(ENABLED);
        assertThat(isVersioningEnabled).isTrue();
    }

    @Test
    void minioClientSaveFile(@Value("classpath:test-file.txt") Resource testResource) throws Throwable {
        InputStream inputStream = testResource.getInputStream();
        ObjectWriteResponse response = minioClient.putObject(
            PutObjectArgs.builder().bucket(minioProperties.getBucket()).object(TEST_FILE_NAME).stream(
                inputStream, inputStream.available(), -1)
                .build());

        List<Item> results = getItemsFromBucket(minioProperties.getBucket(), true);
        for (Item item : results) {
            assertThat(item.objectName()).isEqualTo(TEST_FILE_NAME);
            assertThat(item.versionId()).isEqualTo(response.versionId());
            assertThat(item.isLatest()).isTrue();
        }
    }

    @Test
    void deleteLatestVersion_verifyThatRemainingVersionBecomesLatestVersion(@Value("classpath:test-file.txt") Resource testResource) throws Throwable {
        //Add first version
        InputStream inputStream1 = testResource.getInputStream();
        minioClient.putObject(
            PutObjectArgs.builder().bucket(minioProperties.getBucket()).object(TEST_FILE_NAME).stream(
                inputStream1, inputStream1.available(), -1)
                .build());

        //Add second version
        InputStream inputStream2 = testResource.getInputStream();
        minioClient.putObject(
            PutObjectArgs.builder().bucket(minioProperties.getBucket()).object(TEST_FILE_NAME).stream(
                inputStream2, inputStream2.available(), -1)
                .build());

        //Identify "latest" version
        Item latestItem = null;
        int items = 0;
        for (Item item : getItemsFromBucket(minioProperties.getBucket(), true)) {
            if (item.isLatest()) {
                latestItem = item;
            }
            items++;
        }
        assertThat(items).isEqualTo(2);
        //Delete latest version
        minioClient.removeObject(RemoveObjectArgs.builder().bucket(minioProperties.getBucket()).object(latestItem.objectName()).versionId(latestItem.versionId()).build());
        items = 0;

        //Verify remaining version is now "latest" version
        for (Item item : getItemsFromBucket(minioProperties.getBucket(), true)) {
            assertThat(item.objectName()).isEqualTo(latestItem.objectName());
            assertThat(item.versionId()).isNotEqualTo(latestItem.versionId());
            assertThat(item.isLatest()).isTrue();
            items++;
        }

        assertThat(items).isEqualTo(1);
    }

    @Test
    void deleCurrentVersionAndVerifyThatRemainingVersionBecomesLatestVersion(@Value("classpath:test-file.txt") Resource testResource) throws Throwable {
        //Add first version
        InputStream inputStream1 = testResource.getInputStream();
        minioClient.putObject(
            PutObjectArgs.builder().bucket(minioProperties.getBucket()).object(TEST_FILE_NAME).stream(
                inputStream1, inputStream1.available(), -1)
                .build());

        //Add second version
        InputStream inputStream2 = testResource.getInputStream();
        minioClient.putObject(
            PutObjectArgs.builder().bucket(minioProperties.getBucket()).object(TEST_FILE_NAME).stream(
                inputStream2, inputStream2.available(), -1)
                .build());

        List<Item> items = getItemsFromBucket(minioProperties.getBucket(), false);
        assertThat(items.size()).isEqualTo(1);  //List doesn't include versions
        Item itemToRemove = items.get(0);
        minioClient.removeObject(RemoveObjectArgs.builder().bucket(minioProperties.getBucket()).object(itemToRemove.objectName()).build());
        List<Item> versions = getItemsFromBucket(minioProperties.getBucket(), true);
        for (Item item : versions) {
            log.info("Versions for Object {}, Version {}, LastModified {}, deleteMarker {}, {} ", item.objectName(), item.versionId(), item.lastModified(), item.isDeleteMarker(), item.getClass());
        }
        List<Item> remainingItems = getItemsFromBucket(minioProperties.getBucket(), false);
        assertThat(remainingItems.size()).isEqualTo(0);
    }

    private List<Item> getItemsFromBucket(String bucketName, boolean includeVersions) {
        Iterator<Result<Item>> itemResults = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).includeVersions(includeVersions).build()).iterator();
        List<Result<Item>> items = new ArrayList<>();
        itemResults.forEachRemaining(items::add);
        return items.stream().map(result ->
        {
            try {
                return result.get();
            } catch (Exception ex) {
                throw new RuntimeException();
            }
        }).collect(Collectors.toList());
    }
}
