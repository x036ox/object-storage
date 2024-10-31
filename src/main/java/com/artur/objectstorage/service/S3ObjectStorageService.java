package com.artur.objectstorage.service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.artur.objectstorage.config.S3Config;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

@Slf4j
public class S3ObjectStorageService implements ObjectStorageService{

    private final AmazonS3 s3;
    private final String bucket;
    private final S3Config config;

    public S3ObjectStorageService(S3Config config) {
        this.bucket = config.storeBucket();
        this.config = config;
        this.s3 = s3Client(config);
    }

    private AmazonS3 s3Client(S3Config s3Config){
        AWSCredentials credentials = new BasicAWSCredentials(s3Config.accessKey(), s3Config.secretKey());
        AmazonS3 s3 = AmazonS3Client.builder()
                .withRegion("eu-west-3")
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        initBucket(s3, s3Config.storeBucket());
        return s3;
    }

    private void initBucket(AmazonS3 s3, String bucket) {
        try {
            boolean exists = s3.doesBucketExistV2(bucket);
            if (!exists) {
                s3.createBucket(bucket);
                log.info("Bucket [{}] successfully created", bucket);
            }
        } catch (Exception e){
            log.error("Error occurred while checking or creating bucket [{}]", bucket);
            throw new RuntimeException(e);
        }
    }


    @Override
    public String putObject(InputStream objectInputStream, String objectName) throws Exception {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentDisposition("attachment");
        s3.putObject(bucket, objectName, objectInputStream, objectMetadata);
        return objectName;
    }

    @Override
    public void uploadObject(File object, String pathname) throws Exception {
        if(pathname.startsWith("/")){
            pathname = pathname.replaceFirst("/", "");
        }
        if(!pathname.endsWith("/")){
            pathname += '/';
        }
        s3.putObject(bucket, pathname + object.getName(), object);
    }

    @Override
    public void putFolder(String folderName) throws Exception {
        s3.putObject(bucket, folderName, (File) null);
    }

    @Override
    public List<String> listFiles(String prefix) throws Exception {
        return s3.listObjectsV2(bucket, prefix)
                .getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .toList();
    }

    @Override
    public InputStream getObject(String objectName) throws Exception {
        return s3.getObject(bucket, objectName).getObjectContent();
    }

    @Override
    public void removeObject(String objectName) throws Exception {
        s3.deleteObject(bucket,objectName);
    }

    @Override
    public void removeFolder(String prefix) throws Exception {
        s3.listObjects(bucket, prefix).getObjectSummaries().forEach(object ->{
            s3.deleteObject(bucket, object.getKey());
        });
    }

    @Override
    public String getObjectUrl(String objectName) throws Exception {
        return s3.getUrl(bucket, objectName).toExternalForm();
    }

    @Override
    public Instant getLastModified(String objectName) {
        return s3.getObject(bucket, objectName).getObjectMetadata().getLastModified().toInstant();
    }
}
