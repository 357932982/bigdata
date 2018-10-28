package com.bigdata.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsStreamAccess {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        conf.set("dfs.replication", "2");

        fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"), conf, "root");
    }

    // 流的方式上传文件
    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path("/testIOCopy.txt"));
        FileInputStream inputStream = new FileInputStream("f:/testIOCopy.txt");

        IOUtils.copy(inputStream, outputStream);
        System.out.println("OK");
    }

    // 流的方式下载文件
    @Test
    public void testDownload() throws URISyntaxException, IOException {
        FSDataInputStream inputStream = fs.open(new Path(new URI("/testIOCopy.txt")));
        FileOutputStream outputStream = new FileOutputStream("f:/testIOCopy.txt");
        IOUtils.copy(inputStream, outputStream);
        System.out.println("ok");
    }

    // 流的方式随机读取文件
    @Test
    public void testRandomAccess() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/testIOCopy.txt"));
        inputStream.seek(20);
        FileOutputStream outputStream = new FileOutputStream("f:/test.txt");
        // IOUtils.copyLarge(inputStream, outputStream, 0, 10);
        IOUtils.copy(inputStream, outputStream);
        System.out.println("ok");

    }

    // 读取文件显示到控制台
    @Test
    public void testCat() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/testIOCopy.txt"));
        IOUtils.copy(inputStream, System.out);
    }
}
