package com.bigdata.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SimpleZkClient {

    private static final String connectString = "192.168.196.134:2181, 192.168.196.135:2181, 192.168.196.136:2181,";
    private static final int sessionTimeout = 2000;
    ZooKeeper zkClient = null;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 收到时间通知后的回调函数
                System.out.println(event.getType() + "---" + event.getPath());
            }
        });
    }

    /**
     * 创建数据节点到zookeeper中
     * 参数1： 要创建的节点的路径
     * 参数2： 节点的数据
     * 参数3： 节点的权限
     * 参数4： 节点的类型（临时、持久等）
     */
    @Test
    public void testCreate() throws KeeperException, InterruptedException {

        String nodeCreated = zkClient.create("/eclipse", "hello zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println(nodeCreated);

    }

    // 判断znode是否存在
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/eclipse", false);
        System.out.println(stat == null ?"not exist":"exist");
    }

    // 获取子节点
    @Test
    public void getChildNodes() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        System.out.println(children);
    }

    // 获取znode的数据
    @Test
    public void getData() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/eclipse", false, null);
        System.out.println(new String(data));
    }

    // 删除znode节点
    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        //参数2：指定要删除的版本，-1表示删除所有版本
        zkClient.delete("/eclipse", -1);
    }

    // 修改znode
    @Test
    public void setZnode() throws KeeperException, InterruptedException {
        Stat stat = zkClient.setData("/eclipse", "hello world!".getBytes(), -1);
        getData();
    }
}
