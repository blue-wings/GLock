package com.personal.GLock.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * User: FR
 * Time: 13-12-12 下午4:36
 */
public class GReadWriteLock implements ReadWriteLock {

    private ZooKeeper zooKeeper;
    private String lockKey;

    public GReadWriteLock(ZooKeeper zooKeeper, String lockKey) {
        this.zooKeeper = zooKeeper;
        this.lockKey = lockKey;
        try {
            if(zooKeeper.exists(PathIndex.READ_LOCK_NODE_PATH+PathIndex.SPLITER+lockKey, false) == null){
                zooKeeper.create(PathIndex.READ_LOCK_NODE_PATH+PathIndex.SPLITER+lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if(zooKeeper.exists(PathIndex.WRITE_LOCK_NODE_PATH+PathIndex.SPLITER+lockKey, false) == null){
                zooKeeper.create(PathIndex.WRITE_LOCK_NODE_PATH+PathIndex.SPLITER+lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            if(zooKeeper.exists(PathIndex.WAITING_QUEUE_NODE_PATH+PathIndex.SPLITER+lockKey, false) == null){
                zooKeeper.create(PathIndex.WAITING_QUEUE_NODE_PATH+PathIndex.SPLITER+lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            if(zooKeeper.exists(PathIndex.WAKEUP_QUEUE_NODE_PATH+PathIndex.SPLITER+lockKey, false) == null){
                zooKeeper.create(PathIndex.WAKEUP_QUEUE_NODE_PATH+PathIndex.SPLITER+lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Lock readLock() {
        return new GLock(lockKey, false, zooKeeper);
    }

    @Override
    public Lock writeLock() {
        return new GLock(lockKey, true, zooKeeper);
    }
}
