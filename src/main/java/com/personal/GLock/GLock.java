package com.personal.GLock;

import org.apache.zookeeper.ZooKeeper;

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
    ThreadLocal<ZLockQueue> zLockQueueThreadLocal = new ThreadLocal<ZLockQueue>();

    public GLock(String lockKey, Boolean writeLock, ZooKeeper zooKeeper) {
        this.lockKey = lockKey;
        isWriteLock = writeLock;
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void lock() {
        if (zLockQueueThreadLocal.get() == null) {
            ZLockQueue zLockQueue = new ZLockQueue(zooKeeper, lockKey, isWriteLock);
            zLockQueueThreadLocal.set(zLockQueue);
            zLockQueue.getMyTurn(true, 0, null);
        }
        zLockQueueThreadLocal.get().lockTimesInc();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock();
    }

    @Override
    public boolean tryLock() {
        if (zLockQueueThreadLocal.get() == null) {
            ZLockQueue zLockQueue = new ZLockQueue(zooKeeper, lockKey, isWriteLock);
            zLockQueueThreadLocal.set(zLockQueue);
            if(!zLockQueue.getMyTurn(true, 0, null)){
                return false;
            }
        }
        zLockQueueThreadLocal.get().lockTimesInc();
        return true;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (zLockQueueThreadLocal.get() == null) {
            ZLockQueue zLockQueue = new ZLockQueue(zooKeeper, lockKey, isWriteLock);
            zLockQueueThreadLocal.set(zLockQueue);
            if(!zLockQueue.getMyTurn(true, time, unit)){
                return false;
            }
        }
        zLockQueueThreadLocal.get().lockTimesInc();
        return true;
    }

    @Override
    public void unlock() {
        if(zLockQueueThreadLocal.get().lockTimesDec()==0){
            zLockQueueThreadLocal.get().remove();
        }
    }

    @Override
    public Condition newCondition() {
        return new GCondition();
    }

}
