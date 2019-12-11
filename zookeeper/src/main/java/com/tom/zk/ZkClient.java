package com.tom.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: zkClient
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/18 20:12
 */
public class ZkClient {

    private static String connect = "hadoop201:2181,hadoop202:2181,hadoop203:2181";
    private static int timeout = 2000;
    private static ZooKeeper zooKeeper = null;
    private static String parentPath = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //获取客户端
        getClient();
        //获取服务器列表
        getServers();
        //业务逻辑
        business();
    }

    private static void business() throws InterruptedException {
        System.out.println("Client is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void getServers() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(parentPath, true);
        //创建一个集合封装主机名
        List<String> hosts = new ArrayList<>();
        for (String child:children){
            byte[] data = zooKeeper.getData(parentPath + "/" + child, false, null);
            hosts.add(new String(data));
        }
        System.out.println(hosts);
    }

    private static ZooKeeper getClient() throws IOException {
        zooKeeper = new ZooKeeper(connect, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getPath()+"==========="+watchedEvent.getType());
                //重新获取服务器列表
                try {
                    getServers();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        return zooKeeper;
    }
}
