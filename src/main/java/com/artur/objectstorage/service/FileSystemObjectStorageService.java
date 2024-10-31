package com.artur.objectstorage.service;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.FileSystemUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Setter
@Getter
public class FileSystemObjectStorageService implements ObjectStorageService{
    private static final Logger logger = LoggerFactory.getLogger(FileSystemObjectStorageService.class);
    private static final String DATA_PATH = "data/";

    private String rootFolder;

    public FileSystemObjectStorageService(String rootFolder){
        this.rootFolder = rootFolder != null ? rootFolder : DATA_PATH;
    }

    public FileSystemObjectStorageService(){
        this.rootFolder = DATA_PATH;
    }

    @Override
    public void removeFolder(String prefix) throws IOException {
        FileSystemUtils.deleteRecursively(Path.of(prefix));
    }

    @Override
    public String getObjectUrl(String objectName) throws Exception {
        return null;
    }

    @Override
    public Instant getLastModified(String objectName) throws Exception {
        return Instant.ofEpochMilli(new File(objectName).lastModified());
    }

    @Override
    public String putObject(InputStream inputStream, String objectName) throws IOException {
        Assert.notNull(inputStream, "Input stream can not be null");
        Assert.notNull(objectName, "Name can not be null");
        Assert.isTrue(objectName.contains("."), "Name should be with extension");
        if(objectName.contains("/")){
            putFolder(objectName.substring(0, objectName.lastIndexOf("/")));
        }
        File file = new File(rootFolder + objectName);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            inputStream.transferTo(fileOutputStream);
        }
        return objectName;
    }

    @Override
    public void uploadObject(File object, String pathname) throws Exception {
        try (InputStream inputStream = new FileInputStream(object)){
            putObject(inputStream, pathname);
        }
    }

    @Override
    public void putFolder(String prefix) {
        Path path = Path.of(rootFolder + prefix);
        if(Files.notExists(path)){
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                logger.error("Could not create directory [ " + path + " ]", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<String> listFiles(String prefix) throws Exception {
        File directory = Path.of(rootFolder, prefix).toFile();
        if(!directory.isDirectory()){
            throw new IllegalArgumentException("Could not get list of file because [ " + prefix + " ] is not a directory");
        }
        return Arrays.stream(Objects.requireNonNull(directory.listFiles())).map(File::getName).toList();
    }

    @Override
    public InputStream getObject(String objectName) throws Exception {
        return new FileInputStream(rootFolder + objectName);
    }

    @Override
    public void removeObject(String objectName) throws Exception {
        Files.deleteIfExists(Path.of(rootFolder + objectName));
    }
}
