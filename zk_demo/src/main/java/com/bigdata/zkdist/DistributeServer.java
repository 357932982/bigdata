package com.bigdata.zkdist;

import org.apache.zookeeper.*;

public class DistributeServer {

    private static final String connectString = "192.168.196.134:2181, 192.168.196.135:2181, 192.168.196.136:2181,";
    private static final int sessionTimeout = 2000;
    private static final String parentNode = "/servers";
    private ZooKeeper zk = null;
    /**
     *  创建到zk的客户端连接
     */
    public void getConnected() throws Exception {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到时间通知后的回调函数
                System.out.println(event.getType() + "---" + event.getPath());
                try {
                    zk.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 向zookeeper注册服务器
     * @param serverName 服务器名
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void registerServer(String serverName) throws KeeperException, InterruptedException {
        String create = zk.create(parentNode + "/server", serverName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(serverName + " is on line "+ create);
    }

    /**
     * 业务功能
     */
    public void handleBussiness(String serverName) throws InterruptedException {
        System.out.println(serverName +" is start working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        // 获取链接
        DistributeServer server = new DistributeServer();
        server.getConnected();

        // 注册到zookeeper
        server.registerServer(args[0]);

        // 启动业务功能
        server.handleBussiness(args[0]);

    }
}
