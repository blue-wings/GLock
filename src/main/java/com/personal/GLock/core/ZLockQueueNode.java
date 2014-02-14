package com.personal.GLock.core;

import com.personal.GLock.state.ZNodeState;
import com.personal.GLock.util.Config;
import com.personal.GLock.util.EnhancedZookeeper;
import com.personal.GLock.util.EnhancedZookeeperResult;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * User: FR
 * Time: 13-12-12 下午4:34
 */
public class ZLockQueueNode {
    private final ZooKeeper zooKeeper;
    private final String lockKey;
    private final boolean isWriteType;

    private String node;
    /**
     * this node delete action cause back nodes wake up
     * two case
     * 1.   this node is first write node before read nodes;
     * 2.   this node is prior write node before write node;
     */
    private String certainPreWriteNode;
    /**
     * this nodes set generate cat this case
     * there are some read nodes before write node, zhe write node must listen all read node before delete event.
     */
    private List<String> writeNodePreReadNodes = new ArrayList<String>();
    private int preReadNodesDeleteEventReceivedCount = 0;
    /**
     * reentrant times
     */
    private int lockTimes;
    /**
     * this flag show weather get lock action is after wake up
     * two case
     * 1.  wake up is activated by pre node delete event
     * 2.  wake up is activated by wait time up in tryLock(forWait, time, unit)  method
     */
    private boolean startByWakeUp = false;
    /**
     * this flag show wake up is start by pre node delete event
     */
    private boolean startByPreNodeDelWakeUp = false;
    //this flag support try lock action, that main it's node util it's certainPreWriteNode or  writeNodePreReadNodes deleted.
    private boolean isMaintain = false;

    private Logger logger = LoggerFactory.getLogger(ZLockQueueNode.class);

    ZLockQueueNode(ZooKeeper zooKeeper, String lockKey, boolean writeLock) {
        this.zooKeeper = zooKeeper;
        this.lockKey = lockKey;
        isWriteType = writeLock;
        createZNode();
    }

    synchronized ZNodeState getSlot(boolean forWait, long time, TimeUnit unit) {
        if (isWriteType) {
            return getWriteSlot(forWait, time, unit);
        }
        return getReadSlot(forWait, time, unit);
    }

    private ZNodeState getWriteSlot(boolean forWait, long time, TimeUnit unit) {
        logger.debug("start to get write slot " + toString());
        EnhancedZookeeperResult getChildrenResult = EnhancedZookeeper.getChildren(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey, false);
        if (getChildrenResult.isZookeeperError()) {
            logger.error("zookeeper getChildren error, it is impossible unless zookeeper is down");
            return ZNodeState.FAILED;
        }
        List<String> children = getChildrenResult.getChildren();
        Collections.sort(children);
        int index = Collections.binarySearch(children, node);
        certainPreWriteNode=null;
        if (index == 0) {
            logger.debug(" get write slot success  " + toString());
            isMaintain = false;
            return ZNodeState.OK;
        } else if (startByWakeUp && !startByPreNodeDelWakeUp) {
            logger.debug("get write slot expired " + toString());
            isMaintain = true;
            return ZNodeState.WAKE_UP_FAILED;
        } else {
            String preNode = children.get(index - 1);
            EnhancedZookeeperResult dataResult = EnhancedZookeeper.getData(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + preNode, false, null, false);
            if (dataResult.isZookeeperError()) {
                logger.debug("pre node of this write node not exist,may be deleted. " + toString());
                return ZNodeState.FAILED;
            }
            if(dataResult.getNodeIsDelete()!=null && dataResult.getNodeIsDelete()){
                logger.debug("pre node of this write node not exist,may be deleted. " + toString());
                return getWriteSlot(forWait, time, unit);
            }
            if (dataResult.getData() != null && dataResult.getData().equals(Config.WRITE_NODE_DATA)) {
                certainPreWriteNode = preNode;
            }
            //certainPreWriteNode is null, so this node must wait for all before nodes have deleted
            //TODO this future may banding wasted node delete event (w w w w r r r W). optimal point
            if (certainPreWriteNode == null) {
                for (int i = index - 1; i >= 0; i--) {
                    String pre = children.get(i);
                    writeNodePreReadNodes.add(pre);
                }
            }
            if (certainPreWriteNode != null) {
                EnhancedZookeeperResult existResult = EnhancedZookeeper.exist(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + certainPreWriteNode, new preNodeDelWatcher());
                if(existResult.isZookeeperError()){
                    return ZNodeState.FAILED;
                }
                if (existResult.getStat() == null) {
                    return ZNodeState.OK;
                }
            } else {
                Iterator<String> iterator = writeNodePreReadNodes.iterator();
                while (iterator.hasNext()) {
                    String preReadNode = iterator.next();
                    EnhancedZookeeperResult existResult = EnhancedZookeeper.exist(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + preReadNode, new preNodeDelWatcher());
                    if(existResult.isZookeeperError()){
                        return ZNodeState.FAILED;
                    }
                    if (existResult.getStat() == null) {
                        iterator.remove();
                    }
                }
                if (writeNodePreReadNodes.isEmpty()) {
                    return ZNodeState.OK;
                }
            }
        }
        if (forWait) {
            try {
                logger.debug("write waite " + toString());
                if (time == Long.MAX_VALUE) {
                    wait();
                } else {
                    wait(unit.toMillis(time));
                }
                logger.debug("write wake up " + toString());
                startByWakeUp = true;
            } catch (InterruptedException e) {
                return ZNodeState.WAIT_INTERRUPT;
            }
            return getWriteSlot(forWait, time, unit);
        }
        return ZNodeState.FAILED;
    }

    private ZNodeState getReadSlot(boolean forWait, long time, TimeUnit unit) {
        logger.debug("start to get read slot " + toString());
        EnhancedZookeeperResult getChildrenResult = EnhancedZookeeper.getChildren(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey, false);
        if (getChildrenResult.isZookeeperError()) {
            return ZNodeState.FAILED;
        }
        List<String> children = getChildrenResult.getChildren();
        Collections.sort(children);
        int index = Collections.binarySearch(children, node);
        certainPreWriteNode = null;
        for (int i = index - 1; i >= 0; i--) {
            String pre = children.get(i);
            EnhancedZookeeperResult getDataResult = EnhancedZookeeper.getData(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + pre, false, null, false);
            if (getDataResult.isZookeeperError()) {
                return ZNodeState.FAILED;
            }
            if(getDataResult.getNodeIsDelete()){
                continue;
            }
            String data = getDataResult.getData();
            if (data != null && data.equals(Config.WRITE_NODE_DATA)) {
                certainPreWriteNode = pre;
                break;
            }
        }
        if (certainPreWriteNode == null) {
            //two condition,one first in this method that there is any write node before;two wake up by write node delete event,so it get lock certainly
            logger.debug(" get read lock " + toString());
            return ZNodeState.OK;
        } else if (startByWakeUp && !startByPreNodeDelWakeUp) {
            //wake up by wait time up, but can not get lock yet,so give up.
            logger.debug("get read slot expired " + toString());
            isMaintain = true;
            return ZNodeState.WAKE_UP_FAILED;
        } else {
            EnhancedZookeeperResult existResult = EnhancedZookeeper.exist(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + certainPreWriteNode, new preNodeDelWatcher());
            if(existResult.isZookeeperError()){
                return ZNodeState.FAILED;
            }
            if (existResult.getStat() == null) {
                logger.debug(" get read lock " + toString());
                return ZNodeState.OK;
            }
        }
        if (forWait) {
            try {
                logger.debug("read waite " + toString());
                if (time == Long.MAX_VALUE) {
                    wait();
                } else {
                    wait(unit.toMillis(time));
                }
                logger.debug("read wake up " + toString());
                startByWakeUp = true;
            } catch (InterruptedException e) {
                return ZNodeState.WAIT_INTERRUPT;
            }
            return getReadSlot(forWait, time, unit);
        }
        return ZNodeState.FAILED;
    }

    synchronized void remove() {
        if (!isMaintain) {
            EnhancedZookeeperResult existResult = EnhancedZookeeper.exist(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + node, false);
            if (existResult.getStat() != null) {
                EnhancedZookeeper.deleteNode(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER + node, -1);
                logger.debug(node + " remove node success");
            }
        }
    }

    private class preNodeDelWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            synchronized (ZLockQueueNode.this) {
                boolean action = false;
                if (certainPreWriteNode != null) {
                    action = true;
                } else if (!writeNodePreReadNodes.isEmpty() && (++preReadNodesDeleteEventReceivedCount == writeNodePreReadNodes.size())) {
                    action = true;
                }
                if (action) {
                    if (isMaintain) {
                        isMaintain = false;
                        remove();
                    } else {
                        startByPreNodeDelWakeUp = true;
                        ZLockQueueNode.this.notify();
                    }
                }
            }
        }
    }

    ZNodeState reInQueue(boolean forWait, long time, TimeUnit unit) {
        createZNode();
        return getSlot(forWait, time, unit);
    }

    private void createZNode() {
        String data = isWriteType ? Config.WRITE_NODE_DATA : Config.READ_NODE_DATA;
        String superPath = Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey;
        String createPath = Config.WRITE_LOCK_NODE_PATH + Config.SPLITTER + lockKey + Config.SPLITTER;
        EnhancedZookeeperResult result = EnhancedZookeeper.create(zooKeeper, createPath, superPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        node = result.getNode().substring(result.getNode().lastIndexOf(Config.SPLITTER) + 1);
    }

    int lockTimesInc() {
        return ++lockTimes;
    }

    int lockTimesDec() {
        return --lockTimes;
    }

    boolean isWriteType() {
        return isWriteType;
    }

    int getLockTimes() {
        return lockTimes;
    }

    String getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "ZLockQueueNode{" +
                ", lockKey='" + lockKey + '\'' +
                ", isWriteType=" + isWriteType +
                ", node='" + node + '\'' +
                ", certainPreWriteNode='" + certainPreWriteNode + '\'' +
                ", writeNodePreReadNodes=" + writeNodePreReadNodes +
                ", preReadNodesDeleteEventReceivedCount=" + preReadNodesDeleteEventReceivedCount +
                ", lockTimes=" + lockTimes +
                ", startByWakeUp=" + startByWakeUp +
                ", startByPreNodeDelWakeUp=" + startByPreNodeDelWakeUp +
                ", isMaintain=" + isMaintain +
                '}';
    }
}
