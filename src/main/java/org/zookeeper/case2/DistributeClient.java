package org.zookeeper.case2;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 服务器动态上下线监听案例 : 客户端
 */
public class DistributeClient {
    private ZooKeeper zooKeeper = null;
    private int sessionTimeout = 200000;//连接时间
    private String connectString = "162.14.77.50:2181,162.14.77.50:2182,162.14.77.50:2183";//服务器地址端口信息
    private String parentNode = "/servers";//服务器所有注册父路径

    //获取zk连接
    public void getZkConnect() throws IOException {
        zooKeeper  = new ZooKeeper(connectString, sessionTimeout, watchedEvent->{
            try {
                getServerList();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    //获取服务器注册列表
    public void getServerList() throws KeeperException, InterruptedException {
        List<String> servers = zooKeeper.getChildren(parentNode, true);
        //存储节点中的主机名称信息
        ArrayList<String> serverList = new ArrayList<>();
        for (String server : servers) {
            //获取节点中的主机名称信息
            byte[] data = zooKeeper.getData(parentNode+"/"+server, false, null);
            serverList.add(new String(data));
        }
        //打印节点信息：
        System.out.println("------------在线的节点-----------------");
        System.out.println(serverList.toString());

    }

    // 业务功能
    public void business() throws Exception{
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    // 测试方法
    @Test
    public void onlineTest() throws Exception {
        String server = "";
        //1获取 zk 连接
        getZkConnect();
        //2获取 servers 的子节点信息，从中获取服务器信息列表
        getServerList();
        //3业务进程启动
        business();
    }
}
