package com.artur.objectstorage.service;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

@Slf4j
public class S3ObjectStorageService implements ObjectStorageService{

    private final S3Client s3;
    private final String bucket;

    public S3ObjectStorageService(S3Client s3, String bucket) {
        this.bucket = bucket;
        this.s3 = s3;
    }

    @Override
    public String putObject(InputStream objectInputStream, String objectName) throws Exception {
        s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectName)
                .build(), RequestBody.fromInputStream(objectInputStream, -1));
        return objectName;
    }

    @Override
    public void uploadObject(File object, String pathname) throws Exception {
        if(pathname.startsWith("/")){
            pathname = pathname.replaceFirst("/", "");
        }
        if(!pathname.isEmpty() && !pathname.endsWith("/")){
            pathname += '/';
        }
        s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(pathname + object.getName())
                .build(), RequestBody.fromFile(object));
    }

    @Override
    public void putFolder(String folderName) throws Exception {
        s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(folderName)
                .build(), RequestBody.empty());
    }

    @Override
    public List<String> listFiles(String prefix) throws Exception {
        return s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(prefix)
                        .build())
                .contents().stream()
                .map(S3Object::key)
                .toList();
    }

    @Override
    public InputStream getObject(String objectName) throws Exception {
        return s3.getObject(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectName)
                .build());
    }

    @Override
    public void removeObject(String objectName) throws Exception {
        s3.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectName)
                .build());
    }

    @Override
    public void removeFolder(String prefix) throws Exception {
        s3.listObjects(
                ListObjectsRequest.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build()
        ).contents().forEach(object -> s3.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(object.key())
                .build()));
    }

    @Override
    public String getObjectUrl(String objectName) throws Exception {
        return s3.utilities().getUrl(builder -> builder.bucket(bucket).key(objectName)).toExternalForm();
    }

    @Override
    public Instant getLastModified(String objectName) {
        return s3.headObject(HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectName)
                .build()).lastModified();
    }
}
