package com.eappcat.flink.http;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class RequestUtils {
    public static String get(String path) throws IOException{
        URL url=new URL(path);
        HttpURLConnection connection=(HttpURLConnection)url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
        connection.connect();
        int code = connection.getResponseCode();
        if (code < 200 || code >=300){
            throw new IOException("code: "+code+", message:"+connection.getResponseMessage());
        }
        return IOUtils.toString( connection.getInputStream(),Charsets.UTF_8);
    }
}
