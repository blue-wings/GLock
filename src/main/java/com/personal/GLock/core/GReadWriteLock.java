package com.personal.GLock.core;

import com.personal.GLock.util.Config;
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
            if(zooKeeper.exists(Config.READ_LOCK_NODE_PATH+ Config.SPLITTER +lockKey, false) == null){
                zooKeeper.create(Config.READ_LOCK_NODE_PATH+ Config.SPLITTER +lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if(zooKeeper.exists(Config.WRITE_LOCK_NODE_PATH+ Config.SPLITTER +lockKey, false) == null){
                zooKeeper.create(Config.WRITE_LOCK_NODE_PATH+ Config.SPLITTER +lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            if(zooKeeper.exists(Config.WAITING_QUEUE_NODE_PATH+ Config.SPLITTER +lockKey, false) == null){
                zooKeeper.create(Config.WAITING_QUEUE_NODE_PATH+ Config.SPLITTER +lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            if(zooKeeper.exists(Config.WAKEUP_QUEUE_NODE_PATH+ Config.SPLITTER +lockKey, false) == null){
                zooKeeper.create(Config.WAKEUP_QUEUE_NODE_PATH+ Config.SPLITTER +lockKey,  lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
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
