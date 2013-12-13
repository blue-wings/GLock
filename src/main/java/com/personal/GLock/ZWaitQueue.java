package com.personal.GLock;

import org.apache.zookeeper.*;

/**
 * User: FR
 * Time: 12/13/13 11:23 AM
 */
public class ZWaitQueue {

    private ZooKeeper zooKeeper;
    private ZLockQueue zLockQueue;
    private String lockKey;

    private String node;

    public ZWaitQueue(ZooKeeper zooKeeper, ZLockQueue zLockQueue, String lockKey) {
        this.zooKeeper = zooKeeper;
        this.zLockQueue = zLockQueue;
        this.lockKey = lockKey;
    }

    synchronized void inWait() {
        zLockQueue.remove();
        try {
            node = zooKeeper.create(PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER, PathIndex.WAIT_NODE_DATA.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            node = node.substring(node.lastIndexOf(PathIndex.SPLITER) + 1);
            zooKeeper.exists(PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER + node, new nodeDelWatcher());
            wait();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zLockQueue.reInQueue(false, 0, null);
    }

    private class nodeDelWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            synchronized (ZWaitQueue.this) {
                System.out.println("get waiting node delete signal");
                ZWaitQueue.this.notify();
            }
        }
    }

}
