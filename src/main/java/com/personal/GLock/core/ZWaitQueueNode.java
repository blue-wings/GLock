package com.personal.GLock.core;

import com.personal.GLock.state.ZNodeState;
import com.personal.GLock.util.Config;
import com.personal.GLock.util.EnhancedZookeeper;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: FR
 * Time: 12/13/13 11:23 AM
 */
public class ZWaitQueueNode {

    private ZooKeeper zooKeeper;
    private ZLockQueueNode zLockQueueNode;
    private String lockKey;

    private String node;

    private Logger logger = LoggerFactory.getLogger(ZLockQueueNode.class);

    public ZWaitQueueNode(ZooKeeper zooKeeper, ZLockQueueNode zLockQueueNode, String lockKey) {
        this.zooKeeper = zooKeeper;
        this.zLockQueueNode = zLockQueueNode;
        this.lockKey = lockKey;
    }

    synchronized ZNodeState inWait() {
        zLockQueueNode.remove();

        String superPath = Config.WAITING_QUEUE_NODE_PATH + Config.SPLITTER + lockKey;
        String createPath = Config.WAITING_QUEUE_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER;
        EnhancedZookeeper.create(zooKeeper, createPath, superPath, Config.WAIT_NODE_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        node = node.substring(node.lastIndexOf(Config.SPLITTER) + 1);
        EnhancedZookeeper.exist(zooKeeper, Config.WAITING_QUEUE_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + node, new nodeDelWatcher());

        try {
            wait();
            return zLockQueueNode.reInQueue(true, Long.MAX_VALUE, null);
        } catch (InterruptedException e) {
            EnhancedZookeeper.deleteNode(zooKeeper, Config.WAITING_QUEUE_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + node, -1);
            logger.debug(e.getMessage());
            return ZNodeState.WAIT_INTERRUPT;
        }
    }

    private class nodeDelWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            synchronized (ZWaitQueueNode.this) {
                logger.debug(" get waiting node delete signal");
                ZWaitQueueNode.this.notify();
            }
        }
    }

}
