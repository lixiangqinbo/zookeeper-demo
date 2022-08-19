package org.zookeeper.case3;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * CurartorLock实现分布式锁
 */
public class CuratorLock {

    private int sessionTimeout = 200000; //会话超时时间
    private int connectionTimeout = 200000; // connection连接超时时间
    private String connectString = "162.14.77.50:2181,162.14.77.50:2182,162.14.77.50:2183"; //zookeeper服务列表
    private String lockNode = "/lock"; //服务器所有注册父路径


    // 分布式锁初始化
    public CuratorFramework getCuratorFramework () {
        //重试策略，初始时间3s ，重试3次
        RetryPolicy policy = new ExponentialBackoffRetry(3000, 3);
        //通过工厂创建Curator
//        CuratorFramework curator = CuratorFrameworkFactory.newClient(connectString,
//                connectionTimeout,sessionTimeout,policy);
        CuratorFramework curator = CuratorFrameworkFactory.builder().connectionTimeoutMs(connectionTimeout)
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(policy)
                .build();
        //开启连接
        curator.start();
        System.out.println("zookeeper.........初始化完成!");
        return curator;
    }

    public void operateLock(){
        //创建分布式锁 1
           InterProcessLock lock1 = new InterProcessMutex(getCuratorFramework(), lockNode);
        //创建分布式锁 2
          InterProcessLock lock2 = new InterProcessMutex(getCuratorFramework(), lockNode);

        new Thread(()->{
        //获取锁对象
            try {
                lock1.acquire();
                System.out.println("线程1拿到锁");
                // 测试锁重入
                lock1.acquire();
                System.out.println("线程1再次拿到锁");
                System.out.println("线程2请求获取锁---获取不到将会堵塞....设置5s没拿到自动放弃请求锁");
                lock2.acquire(5, TimeUnit.SECONDS);//五秒后没拿到就自动放弃
                //5s 后释放锁
                Thread.sleep(5 * 1000);
                lock1.release();
                System.out.println("线程1释放锁");
                lock1.release();
                System.out.println("线程1再次释放锁");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(()->{
            try {
                lock2.acquire();
                System.out.println("线程2拿到锁");
                // 测试锁重入
                lock2.acquire();
                System.out.println("线程2再次拿到锁");
                //5s 后释放锁
                Thread.sleep(5 * 1000);
                lock2.release();
                System.out.println("线程2释放锁");
                lock2.release();
                System.out.println("线程2再次释放锁");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    // 测试 之前再单元测试里面 测试 单元测试不支持多线程。。。
    public static void main(String[] args) {
        CuratorLock curatorLock = new CuratorLock();
        curatorLock.operateLock();
    }
}
