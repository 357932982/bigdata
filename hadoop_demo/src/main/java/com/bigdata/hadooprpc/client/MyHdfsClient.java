package com.bigdata.hadooprpc.client;

import com.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyHdfsClient {
    public static void main(String[] args) throws IOException {
        ClientNamenodeProtocol nameNode = RPC.getProxy(ClientNamenodeProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
        String metaData = nameNode.getMetaData("/test");
        System.out.println(metaData);

    }
}
