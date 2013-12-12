package com.personal.GLock;

import org.apache.zookeeper.ZooKeeper;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * User: FR
 * Time: 13-12-12 下午4:31
 */
public class GCondition implements Condition {

    private ZooKeeper zooKeeper;



    @Override
    public void await() throws InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
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
    public void signal() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void signalAll() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
