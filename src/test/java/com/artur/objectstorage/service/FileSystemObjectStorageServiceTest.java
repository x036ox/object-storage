package com.artur.objectstorage.service;

import org.junit.jupiter.api.Test;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileSystemObjectStorageServiceTest {
    private static final File IMAGE = new File("src/test/files/monky.jpg");

    ObjectStorageService objectStorageService;

    @Test
    void putObject() throws Exception {
        String rootFolder = "data/";
        objectStorageService = new FileSystemObjectStorageService(rootFolder);
        String objectName = "folder/some-image." + StringUtils.getFilenameExtension(IMAGE.getPath());
        Path path = Path.of(rootFolder + objectName);
        try (FileInputStream fileInputStream = new FileInputStream(IMAGE)){
            objectStorageService.putObject(fileInputStream, objectName);
            assertTrue(Files.exists(path));
        } finally {
            Files.delete(path);
        }
    }
}