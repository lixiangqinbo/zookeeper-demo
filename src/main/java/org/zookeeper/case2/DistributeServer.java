package org.zookeeper.case2;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

/**
 * 服务器动态上下线监听案例 : 服务端
 */
public class DistributeServer {
    private ZooKeeper zooKeeper = null;
    private int sessionTimeout = 200000; //连接时间
    private String connectString = "162.14.77.50:2181,162.14.77.50:2182,162.14.77.50:2183"; //服务器地址端口信息
    private String parentNode = "/servers/"; //服务器所有注册父路径

    //创建客户端
    public void getZkConnection() throws IOException {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {

        });
    }
    //注册服务
    public void registServer(String hostName,String hostValue) throws KeeperException, InterruptedException {
        String server = zooKeeper.create(parentNode+hostName, hostValue.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(hostValue+"is online"+server);
    }
    // 模拟业务功能
    public void business(String hostName) throws Exception{
        System.out.println(hostName + " is working ...");
        Thread.sleep(20); //20S 后下线
    }
    // 节点下线
    public void destoryNode(String hostName) throws KeeperException, InterruptedException {
        zooKeeper.delete(parentNode+hostName, -1);//version ：-1 表示 所有本版都可以删除
    }
    // 测试方法
    @Test
    public void onlineTest() throws Exception {
        String hostName = "serverNode8";
        String hostValue = "node8";
        //获取zk连接
        getZkConnection();
        //利用zk注册服务器信息
        registServer(hostName,hostValue);
        //启动业务功能
        business(hostName);
        //节点服务下线
        destoryNode(hostName);
    }
}
