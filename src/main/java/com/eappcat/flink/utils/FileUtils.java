package com.eappcat.flink.utils;

import java.io.File;

public class FileUtils {
    public static File tempDir() {
        return new File(System.getProperty("java.io.tmpdir"));
    }
    public static File jobTempDir() {
        File file=new File(System.getProperty("java.io.tmpdir"),"submitJob");
        if (!file.exists()){
            file.mkdir();
        }
        return file;
    }
}
