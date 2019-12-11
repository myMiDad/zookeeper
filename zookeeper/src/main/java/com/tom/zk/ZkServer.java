package com.tom.zk;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * ClassName: ZkServer
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/18 19:57
 */
public class ZkServer {
    private static String connect = "hadoop201:2181,hadoop202:2181,hadoop203:2181";
    private static int timeout = 2000;
    private static ZooKeeper zooKeeper = null;
    private static String parentPath = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //获取zookeeper客户端
        getClient();
        //服务端在zookeeper上创建临时节点，注册
        registServer(args[0]);
        //业务逻辑
        business(args[0]);
    }

    private static void business(String hostname) throws InterruptedException {
        System.out.println(hostname+" is working....");
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void registServer(String hostname) throws KeeperException, InterruptedException {
        //创建临时节点
        String path =
                zooKeeper.create(parentPath + "/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(path+"-------------");


    }

    private static ZooKeeper getClient() throws IOException {
        zooKeeper = new ZooKeeper(connect, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getPath()+"==========="+watchedEvent.getType());
            }
        });
        return zooKeeper;
    }
}
