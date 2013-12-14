package com.personal.GLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * User: FR
 * Time: 13-12-12 下午4:31
 */
public class GCondition implements Condition {

    private ZooKeeper zooKeeper;
    private String lockKey;
    private GLock gLock;

    private Logger logger = LoggerFactory.getLogger(GCondition.class);

    public GCondition(ZooKeeper zooKeeper,GLock gLock, String lockKey) {
        this.zooKeeper = zooKeeper;
        this.gLock = gLock;
        this.lockKey = lockKey;
        logger.debug("condition init complete");
    }

    @Override
    public void await() throws InterruptedException {
        logger.debug("condition start to await");
        ZWaitQueue zWaitQueue =  new ZWaitQueue(zooKeeper, gLock.getCurrentThreadZLockQueue(), lockKey);
        zWaitQueue.inWait();
    }

    @Override
    public void awaitUninterruptibly() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public synchronized void signal() {
        try {
            List<String> children = zooKeeper.getChildren(PathIndex.WAITING_QUEUE_NODE_PATH + "/" + lockKey, false);
            if(children==null || children.isEmpty()){
                return;
            }
            Collections.sort(children);
            String firstWaitingNode = children.get(0);
            zooKeeper.delete(PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER + firstWaitingNode, -1);
            logger.debug("condition signal success");
        } catch (KeeperException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void signalAll() {
        try {
            List<String> children = zooKeeper.getChildren(PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey, false);
            if(children==null || children.isEmpty()){
                return;
            }
            for(String child : children){
                try {
                    zooKeeper.delete(PathIndex.WAITING_QUEUE_NODE_PATH + PathIndex.SPLITER + lockKey+PathIndex.SPLITER+child, -1);
                }catch (Exception e){
                    continue;
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
