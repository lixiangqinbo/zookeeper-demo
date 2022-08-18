package org.zookeeper.case1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.List;

/**
 * 客户端的连接 创建节点，获取节点，监听等基本操作
 */
public class zkClient {
    // 连接的zookeeper ip地址 客户端端口 不要有空格
    private static String connectString = "162.14.77.50:2181,162.14.77.50:2182,162.14.77.50:2183";
    //连接超时时间，设置过低会连接失败
    private static int sessionTime =200000;
    private ZooKeeper zkClient = null;
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTime, watchedEvent -> {
                //收到事件通知后的回调函数（用户的业务逻辑）：对 "/" 路径监听，程序再没结束前再linux去 create /path 会出发回调函数
                System.out.println(watchedEvent.getType()+"--"+watchedEvent.getPath());
                //再一次监听
                try {
                    List<String> children = zkClient.getChildren("/", true);
                    for(String child : children){
                        System.out.println(child);
                    };
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        });
    }
    // 创建子节点
    @Test
    public void create() throws Exception {
        // 参数 1：要创建的节点的路径； 参数 2：节点数据 ； 参数 3：节点权限 ；参数 4：节点的类型
        String s = zkClient.create("/chengdu/qingyangqu", "taishenglord".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }
    //获取节点
    @Test
    public void getNode() throws KeeperException, InterruptedException {
        //获取节点 参数1.节点路径 2.监听
        List<String> children = zkClient.getChildren("/chengdu", true);
        for (String child : children) {
            System.out.println(child);
        }
        //延时阻塞；方便查看监听节点的变化 ：此时会要配合linux段去增删除 path
        //[zk: localhost:2181(CONNECTED) 24] delete /dalian
        //[zk: localhost:2181(CONNECTED) 25] delete /shanghai

            /*None--null
            shanghai
            chengdu
            dalian
            zookeeper
            BeiJin
            NodeChildrenChanged--/
            shanghai
            chengdu
            zookeeper
            BeiJin
            NodeChildrenChanged--/
            chengdu
            zookeeper
            BeiJin*/

        //延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }
    //判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/chengdu", false);
        System.out.println(exists.toString());

    }
}
