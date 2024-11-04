package com.artur.objectstorage.service;


import io.minio.*;
import io.minio.messages.Item;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


@Getter
@Setter
public class MinioObjectStorageService implements ObjectStorageService {
    private static final Logger logger = LoggerFactory.getLogger(MinioObjectStorageService.class);

    private MinioClient minioClient;
    private String bucket;

    public MinioObjectStorageService(MinioClient minioClient, String bucket){
        this.minioClient = minioClient;
        this.bucket = bucket;
    }

    @Override
    public String putObject(InputStream objectInputStream, String objectName) throws Exception {
        return minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(bucket)
                        .object(objectName)
                        .stream(objectInputStream, -1, 5242880)
                        .build()
        ).object();
    }

    @Override
    public void uploadObject(File object, String pathname) throws Exception {
        minioClient.uploadObject(UploadObjectArgs.builder()
                        .object(pathname)
                        .filename(object.getAbsolutePath())
                        .bucket(bucket)
                .build());
    }

    @Override
    public void putFolder(String folderName) throws Exception {
        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(bucket)
                        .object(folderName)
                        .stream(new ByteArrayInputStream(new byte[] {}), 0, -1)
                        .build());
    }

    @Override
    public List<String> listFiles(String prefix) throws Exception {
        if(!prefix.endsWith("/")){
            prefix += "/";
        }
        List<String> results = new ArrayList<>();
        for (Result<Item> itemResult :
                minioClient.listObjects(
                        ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(false).build())) {
            Item i = itemResult.get();
            if (i.isDir()) continue;
            results.add(i.objectName());
        }
        return results;
    }

    @Override
    public GetObjectResponse getObject(String objectName) throws Exception {
        return minioClient.getObject(GetObjectArgs.builder().bucket(bucket).object(objectName).build());
    }

    @Override
    public void removeObject(String objectName) throws Exception {
        minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(objectName).build());
    }

    @Override
    public void removeFolder(String prefix) throws Exception {
        if(!prefix.endsWith("/")){
            prefix += "/";
        }
        for(var item : minioClient.listObjects(ListObjectsArgs.builder().prefix(prefix).bucket(bucket).build())){
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucket)
                    .object(item.get().objectName())
                    .build());
        }
    }

    @Override
    public String getObjectUrl(String objectName) throws Exception {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .bucket(bucket)
                        .object(objectName)
                        .build()
        );
    }

    @Override
    public Instant getLastModified(String objectName) throws Exception{
        return minioClient.statObject(StatObjectArgs.builder()
                        .bucket(bucket)
                        .object(objectName)
                .build()).lastModified().toInstant();
    }
}
