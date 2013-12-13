package com.personal.GLock;

import com.personal.GLock.Exception.LockUpgradeException;
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
    private static ThreadLocal<ZLockQueue> WRITE_ZLOCKQUEUE_THREADLOCAL = new ThreadLocal<ZLockQueue>();
    private static ThreadLocal<ZLockQueue> READ_ZLOCKQUEUE_THREADLOCAL = new ThreadLocal<ZLockQueue>();

    public GLock(String lockKey, Boolean writeLock, ZooKeeper zooKeeper) {
        this.lockKey = lockKey;
        isWriteLock = writeLock;
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void lock() {
        if(isReadUpgradeToWrite()){
            throw new LockUpgradeException("read lock can not upgrade to write lock");
        }
        ThreadLocal<ZLockQueue> zLockQueueThreadLocal = switchThreadLocal();
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
        if(isReadUpgradeToWrite()){
            return false;
        }
        ThreadLocal<ZLockQueue> zLockQueueThreadLocal = switchThreadLocal();
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
        if(isReadUpgradeToWrite()){
            return false;
        }
        ThreadLocal<ZLockQueue> zLockQueueThreadLocal = switchThreadLocal();
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
        ThreadLocal<ZLockQueue> zLockQueueThreadLocal = switchThreadLocal();
        if(zLockQueueThreadLocal.get()!=null && zLockQueueThreadLocal.get().lockTimesDec()==0){
            zLockQueueThreadLocal.get().remove();
        }
    }

    @Override
    public Condition newCondition() {
        return new GCondition(zooKeeper, this,lockKey);
    }

    ZLockQueue getCurrentThreadZLockQueue(){
        if(switchThreadLocal().get()!=null){
            return switchThreadLocal().get();
        }
        return null;
    }

    private boolean isReadUpgradeToWrite(){
        if(READ_ZLOCKQUEUE_THREADLOCAL.get()!=null && isWriteLock){
            return true;
        }
        return false;
    }

    private boolean isWriteDownGradeToRead(){
        if(WRITE_ZLOCKQUEUE_THREADLOCAL.get()!=null && !isWriteLock){
            return true;
        }
        return false;
    }

    private ThreadLocal<ZLockQueue> switchThreadLocal(){
        if(isWriteLock){
            return WRITE_ZLOCKQUEUE_THREADLOCAL;
        }else if (!isWriteLock && isWriteDownGradeToRead()){
            return WRITE_ZLOCKQUEUE_THREADLOCAL;
        }else {
            return READ_ZLOCKQUEUE_THREADLOCAL;
        }
    }

}
