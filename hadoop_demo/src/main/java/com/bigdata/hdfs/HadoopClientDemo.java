package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map.Entry;


/**
 *
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 *
 * 也可以在构造客户端fs对象时，通过参数传递进去
 * @author
 *
 */
public class HadoopClientDemo {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://hadoop-cluster:9000");

        // 拿到一个文件系统的客户端实例对象
        // fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"), conf, "root");
    }

    // 测试上传
    @Test
    public void testUpload() throws IOException {
        // fs.copyFromLocalFile(new Path("f:/考研数学公式手册随身看.pdf"), new Path("/考研数学公式手册随身看.pdf"));
        fs.copyFromLocalFile(new Path("/home/media/Ubuntu/test"), new Path("/test"));

        fs.close();
    }

    // 测试下载
    @Test
    public void testDownload() throws IOException {
        // fs.copyToLocalFile(true, new Path("/jdk/jdk1.8.exe"), new Path("f:/jdk8.exe"));
        // fs.copyToLocalFile(true, new Path("/考研数学公式手册随身看.pdf"), new Path("f:/"));
        fs.copyToLocalFile(new Path("/test"), new Path("/home/media/Ubuntu/test.bak"));
        fs.close();
    }

    // 获取参数值
    @Test
    public void testConf(){
        Iterator<Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()){
            Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey()+"----"+entry.getValue());//conf加载的内容
        }
    }

    // 创建目录
    @Test
    public void makdirTest() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
        System.out.println(mkdirs);
    }

    // 删除目录
    @Test
    public void deleteTest() throws IOException {
        boolean delete = fs.delete(new Path("/aaa"), true);//true， 递归删除
        System.out.println(delete);
    }

    // 列出所有文件
    @Test
    public void listTest() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus){
            System.out.println(fileStatus.getPath()+"-----"+fileStatus.toString());
        }
        System.out.println("===================");
        // 递归找到所有文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            String name = fileStatus.getPath().getName();
            Path path = fileStatus.getPath();
            System.out.println(name+"---"+path.toString());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation b : blockLocations){
                System.out.println("块起始偏移量：" + b.getOffset());
                System.out.println("块长度：" + b.getLength());
                // 块所在的节点
                String[] datanodes = b.getHosts();
                for (String dn : datanodes){
                    System.out.println("datanode: "+dn);
                }
            }
            System.out.println("***********************");
        }
    }

}
