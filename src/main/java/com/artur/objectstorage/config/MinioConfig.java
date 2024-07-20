package com.artur.objectstorage.config;

import lombok.Builder;

@Builder
public record MinioConfig (
        String accessKey,
        String secretKey,
        String storeBucket,
        String url
){}
