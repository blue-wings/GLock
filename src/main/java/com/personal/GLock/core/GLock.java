package com.personal.GLock.core;

import com.personal.GLock.exception.LockUpgradeException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * User: FR
 * Time: 13-12-12 下午4:33
 */
public class GLock implements Lock {

    private final String lockKey;
    private final Boolean isWriteLock;
    private final ZooKeeper zooKeeper;
    private static ThreadLocal<ZLockQueueNode> WRITE_ZLOCKQUEUE_THREADLOCAL = new ThreadLocal<ZLockQueueNode>();
    private static ThreadLocal<ZLockQueueNode> READ_ZLOCKQUEUE_THREADLOCAL = new ThreadLocal<ZLockQueueNode>();

    private Logger logger = LoggerFactory.getLogger(GLock.class);

    public GLock(String lockKey, Boolean writeLock, ZooKeeper zooKeeper) {
        this.lockKey = lockKey;
        isWriteLock = writeLock;
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void lock() {
        if (isReadUpgradeToWrite()) {
            throw new LockUpgradeException("read lock can not upgrade to write lock");
        }
        ThreadLocal<ZLockQueueNode> zLockQueueThreadLocal = switchThreadLocal();
        if (zLockQueueThreadLocal.get() == null) {
            ZLockQueueNode zLockQueueNode = new ZLockQueueNode(zooKeeper, lockKey, isWriteLock);
            zLockQueueThreadLocal.set(zLockQueueNode);
            zLockQueueNode.getSlot(true, Long.MAX_VALUE, null);
        }
        zLockQueueThreadLocal.get().lockTimesInc();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock();
    }

    @Override
    public boolean tryLock() {
        if (isReadUpgradeToWrite()) {
            return false;
        }
        ThreadLocal<ZLockQueueNode> zLockQueueThreadLocal = switchThreadLocal();
        if (zLockQueueThreadLocal.get() == null) {
            ZLockQueueNode zLockQueueNode = new ZLockQueueNode(zooKeeper, lockKey, isWriteLock);
            zLockQueueThreadLocal.set(zLockQueueNode);
            if (!zLockQueueNode.getSlot(false, -1, TimeUnit.MICROSECONDS).isSuccess()) {
                logger.debug("try lock failed");
                return false;
            }
        }
        zLockQueueThreadLocal.get().lockTimesInc();
        return true;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (isReadUpgradeToWrite()) {
            return false;
        }
        if (unit == null) {
            unit = TimeUnit.MILLISECONDS;
        }
        ThreadLocal<ZLockQueueNode> zLockQueueThreadLocal = switchThreadLocal();
        if (zLockQueueThreadLocal.get() == null) {
            ZLockQueueNode zLockQueueNode = new ZLockQueueNode(zooKeeper, lockKey, isWriteLock);
            zLockQueueThreadLocal.set(zLockQueueNode);
            if (!zLockQueueNode.getSlot(true, time, unit).isSuccess()) {
                logger.debug("try lock failed");
                return false;
            }
        }
        zLockQueueThreadLocal.get().lockTimesInc();
        return true;
    }

    @Override
    public void unlock() {
        logger.debug("unlock");
        ThreadLocal<ZLockQueueNode> zLockQueueThreadLocal = switchThreadLocal();
        if (zLockQueueThreadLocal.get() != null && zLockQueueThreadLocal.get().lockTimesDec() <= 0) {
            zLockQueueThreadLocal.get().remove();
        }
    }

    @Override
    public Condition newCondition() {
        return new GCondition(zooKeeper, this, lockKey);
    }

    ZLockQueueNode getCurrentThreadZLockQueue() {
        if (switchThreadLocal().get() != null) {
            return switchThreadLocal().get();
        }
        return null;
    }

    private boolean isReadUpgradeToWrite() {
        if (READ_ZLOCKQUEUE_THREADLOCAL.get() != null && isWriteLock) {
            return true;
        }
        return false;
    }

    private boolean isWriteDownGradeToRead() {
        if (WRITE_ZLOCKQUEUE_THREADLOCAL.get() != null && !isWriteLock) {
            return true;
        }
        return false;
    }

    private ThreadLocal<ZLockQueueNode> switchThreadLocal() {
        if (isWriteLock) {
            logger.debug("switch to write queue threadlocal");
            return WRITE_ZLOCKQUEUE_THREADLOCAL;
        } else if (!isWriteLock && isWriteDownGradeToRead()) {
            logger.debug("downgrade to read lock but still use outside wirte lock");
            return WRITE_ZLOCKQUEUE_THREADLOCAL;
        } else {
            logger.debug("switch to read queue threadlocal");
            return READ_ZLOCKQUEUE_THREADLOCAL;
        }
    }

}
