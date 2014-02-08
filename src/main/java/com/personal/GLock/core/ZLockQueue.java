package com.personal.GLock.core;

import com.personal.GLock.state.ZLockQueueState;
import com.personal.GLock.util.Config;
import com.personal.GLock.util.ZookeeperResult;
import com.personal.GLock.util.ZookeeperUtil;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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
public class ZLockQueue {
    private final ZooKeeper zooKeeper;
    private final String lockKey;
    private final boolean isWriteLock;

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
    private boolean maintainNode = false;

    private Logger logger = LoggerFactory.getLogger(ZLockQueue.class);

    ZLockQueue(ZooKeeper zooKeeper, String lockKey, boolean writeLock) {
        this.zooKeeper = zooKeeper;
        this.lockKey = lockKey;
        isWriteLock = writeLock;
        createZNode();
    }

    synchronized ZLockQueueState getMyTurn(boolean forWait, long time, TimeUnit unit) {
        if (isWriteLock) {
            return getWriteMyTurn(forWait, time, unit);
        }
        return getReadMyTurn(forWait, time, unit);
    }

    private ZLockQueueState getWriteMyTurn(boolean forWait, long time, TimeUnit unit) {
        logger.debug("get write my turn " + toString());
        ZookeeperResult getChildrenResult = ZookeeperUtil.getChildren(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey, false);
        List<String> children = getChildrenResult.getChildren();
        Collections.sort(children);
        int index = Collections.binarySearch(children, node);
        if (index == 0) {
            logger.debug(" get write lock " + toString());
            maintainNode = false;
            return ZLockQueueState.OK;
        } else if (startByWakeUp && !startByPreNodeDelWakeUp) {
            logger.debug("try write lock expired " + toString());
            maintainNode = true;
            return ZLockQueueState.WAKE_UP_FAILED;
        } else {
            String preNode = children.get(index - 1);
            ZookeeperResult dataResult = ZookeeperUtil.getData(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER + preNode, false, null);
            if (dataResult.getData() == null) {
                logger.debug("pre node of this write node not exist,may be deleted. " + toString());
                return getMyTurn(forWait, time, unit);
            }
            if (dataResult.getData() != null && new String(dataResult.getData()).equals(Config.WRITE_NODE_DATA)) {
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
                ZookeeperResult existResult = ZookeeperUtil.exist(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER + certainPreWriteNode, new preNodeDelWatcher());
                if (existResult.getStat() == null) {
                    return ZLockQueueState.OK;
                }
            } else {
                Iterator<String> iterator = writeNodePreReadNodes.iterator();
                while (iterator.hasNext()) {
                    String preReadNode = iterator.next();
                    ZookeeperResult existResult = ZookeeperUtil.exist(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER + preReadNode, new preNodeDelWatcher());
                    if (existResult.getStat() == null) {
                        iterator.remove();
                    }
                }
                if (writeNodePreReadNodes.isEmpty()) {
                    return ZLockQueueState.OK;
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
                return ZLockQueueState.WAIT_INTERRUPT;
            }
            return getWriteMyTurn(forWait, time, unit);
        }
        return ZLockQueueState.FAILED;
    }

    private ZLockQueueState getReadMyTurn(boolean forWait, long time, TimeUnit unit) {
        logger.debug("get read my turn " + toString());
        ZookeeperResult getChildrenResult = ZookeeperUtil.getChildren(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey, false);
        List<String> children = getChildrenResult.getChildren();
        Collections.sort(children);
        int index = Collections.binarySearch(children, node);
        certainPreWriteNode = null;
        for (int i = index - 1; i >= 0; i--) {
            String pre = children.get(i);
            ZookeeperResult getDataResult = ZookeeperUtil.getData(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER + pre, false, null);
            byte[] data = getDataResult.getData();
            if (data != null && new String(data).equals(Config.WRITE_NODE_DATA)) {
                certainPreWriteNode = pre;
                break;
            }
        }
        if (certainPreWriteNode == null) {
            //two condition,one first in this method that there is any write node before;two wake up by write node delete event,so it get lock certainly
            logger.debug(" get read lock " + toString());
            return ZLockQueueState.OK;
        } else if (startByWakeUp && !startByPreNodeDelWakeUp) {
            //wake up by wait time up, but can not get lock yet,so give up.
            logger.debug("try read lock expired " + toString());
            maintainNode = true;
            return ZLockQueueState.WAKE_UP_FAILED;
        } else {
            ZookeeperResult existResult = ZookeeperUtil.exist(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER + certainPreWriteNode, new preNodeDelWatcher());
            Stat stat = existResult.getStat();
            if (stat == null) {
                logger.debug(" get read lock " + toString());
                return ZLockQueueState.OK;
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
                return ZLockQueueState.WAIT_INTERRUPT;
            }
            return getReadMyTurn(forWait, time, unit);
        }
        return ZLockQueueState.FAILED;
    }

    void remove() {
        try {
            if (!maintainNode && zooKeeper.exists(Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER + node, false) != null) {
                zooKeeper.delete(Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER + node, -1);
                logger.debug(node + " remove node success");
            }
        } catch (KeeperException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            remove();
        }
    }

    private class preNodeDelWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            synchronized (ZLockQueue.this) {
                boolean action = false;
                if (certainPreWriteNode != null) {
                    action = true;
                } else if (!writeNodePreReadNodes.isEmpty() && (++preReadNodesDeleteEventReceivedCount == writeNodePreReadNodes.size())) {
                    action = true;
                }
                if (action) {
                    if (maintainNode) {
                        maintainNode = false;
                        remove();
                    } else {
                        startByPreNodeDelWakeUp = true;
                        ZLockQueue.this.notify();
                    }
                }
            }
        }
    }

    ZLockQueueState reInQueue(boolean forWait, long time, TimeUnit unit) {
        createZNode();
        return getMyTurn(forWait, time, unit);
    }

    private void createZNode() {
        String data = isWriteLock ? Config.WRITE_NODE_DATA : Config.READ_NODE_DATA;
        ZookeeperResult result = ZookeeperUtil.create(zooKeeper, Config.WRITE_LOCK_NODE_PATH + Config.SPLITER + lockKey + Config.SPLITER, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        node = result.getNode().substring(result.getNode().lastIndexOf(Config.SPLITER) + 1);
    }

    int lockTimesInc() {
        return ++lockTimes;
    }

    int lockTimesDec() {
        return --lockTimes;
    }

    boolean isWriteLock() {
        return isWriteLock;
    }

    int getLockTimes() {
        return lockTimes;
    }

    String getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "ZLockQueue{" +
                "zooKeeper=" + zooKeeper +
                ", lockKey='" + lockKey + '\'' +
                ", isWriteLock=" + isWriteLock +
                ", node='" + node + '\'' +
                ", certainPreWriteNode='" + certainPreWriteNode + '\'' +
                ", writeNodePreReadNodes=" + writeNodePreReadNodes +
                ", preReadNodesDeleteEventReceivedCount=" + preReadNodesDeleteEventReceivedCount +
                ", lockTimes=" + lockTimes +
                ", startByWakeUp=" + startByWakeUp +
                ", startByPreNodeDelWakeUp=" + startByPreNodeDelWakeUp +
                ", maintainNode=" + maintainNode +
                '}';
    }
}
