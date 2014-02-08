package com.personal.GLock.core;

import com.personal.GLock.state.ZLockQueueState;
import com.personal.GLock.util.ZookeeperUtil;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: FR
 * Time: 12/13/13 11:23 AM
 */
public class ZWaitQueue {

    private ZooKeeper zooKeeper;
    private ZLockQueue zLockQueue;
    private String lockKey;

    private String node;

    private Logger logger = LoggerFactory.getLogger(ZLockQueue.class);

    public ZWaitQueue(ZooKeeper zooKeeper, ZLockQueue zLockQueue, String lockKey) {
        this.zooKeeper = zooKeeper;
        this.zLockQueue = zLockQueue;
        this.lockKey = lockKey;
    }

    synchronized ZLockQueueState inWait() {
        zLockQueue.remove();

        ZookeeperUtil.create(zooKeeper, PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER, PathIndex.WAIT_NODE_DATA.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        node = node.substring(node.lastIndexOf(PathIndex.SPLITER) + 1);
        ZookeeperUtil.exist(zooKeeper, PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER + node, new nodeDelWatcher());

        try {
            wait();
            return zLockQueue.reInQueue(true, Long.MAX_VALUE, null);
        } catch (InterruptedException e) {
            ZookeeperUtil.deleteNode(zooKeeper, PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER + node, -1);
            logger.debug(e.getMessage());
            return ZLockQueueState.WAIT_INTERRUPT;
        }
    }

    private class nodeDelWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            synchronized (ZWaitQueue.this) {
                logger.debug(" get waiting node delete signal");
                ZWaitQueue.this.notify();
            }
        }
    }

}
