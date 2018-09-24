package com.bigdata.zkdist;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {

    private static final String connectString = "192.168.196.134:2181, 192.168.196.135:2181, 192.168.196.136:2181,";
    private static final int sessionTimeout = 2000;
    private static final String parentNode = "/servers";

    private volatile List<String> serverList;
    private ZooKeeper zkClient = null;

    /**
     * 获取链接
     * @throws IOException
     */
    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到时间通知后的回调函数
                System.out.println(event.getType() + "---" + event.getPath() + "---" + event.getState() + "---" + event.getWrapper());
                // 重新刷新服务器列表，并重新监听
                try {
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void getServerList() throws KeeperException, InterruptedException {

        // 获取服务器子节点信息，并且对父节点进行监听
        List<String> serverList = zkClient.getChildren(parentNode,true );

        // 先创建一个局部的list来存服务器信息，防止serverList中的数据在各个客户端不同步
        List<String> servers = new ArrayList<String>();
        for (String child : serverList) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        // 把servers赋值给成员变量serverList，已提供给各业务线程使用
        serverList = servers;

        // 打印出服务器列表
        System.out.println(serverList);
    }

    public void handleBusiness() throws InterruptedException {
        System.out.println("client start working....");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 获取zk连接
        DistributeClient client = new DistributeClient();
        client.getConnect();
        // 获取servers的子节点信息（并监听），从中获取服务器信息列表
        client.getServerList();

        // 业务线程启动
        client.handleBusiness();
    }
}
