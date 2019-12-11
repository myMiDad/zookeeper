package com.tom.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * ClassName: Demo
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/18 18:41
 */
public class Demo {

    private String connect = "hadoop201:2181,hadoop202:2181,hadoop203:2181";
    private int timeout = 2000;
    ZooKeeper zooKeeper = null;

    /**
     * 初始化获取客户端
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(connect, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getPath() + "--------" + watchedEvent.getType());
                try {
                    zooKeeper.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 创建节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void createTest() throws KeeperException, InterruptedException {
//        String path = zooKeeper.create("/firstDemo", "Java".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        String path = zooKeeper.create("/xiaowang1", "hello zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        System.out.println(path);

    }

    /**
     * 获取所有子节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getTest() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", true);
        for (String c:children){
            System.out.println(c);
        }
//        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 查看节点是否存在
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void existsTest() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/eclipse",false);
        System.out.println(stat == null ? "not exists":"exists");
    }

    /**
     * 改变节点内容
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void setTest() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.setData("/test", "zhangsan shige mingren".getBytes(), -1);
        System.out.println(stat == null );
    }
    @Test
    public void getDataTest() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/test", true, null);
        System.out.println(new String(data));
//        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void deleteTest() throws KeeperException, InterruptedException {
        zooKeeper.delete("/idea",-1);
    }

}
