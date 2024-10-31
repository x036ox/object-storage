package com.artur.objectstorage.config;

import lombok.Builder;

@Builder
public record S3Config(
        String accessKey,
        String secretKey,
        String storeBucket,
        String region
){}
