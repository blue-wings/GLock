package com.personal.GLock;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

/**
 * User: FR
 * Time: 13-12-12 下午4:38
 */
public class Test {
    private static String HOSTS = "172.27.22.112:2181,172.27.22.112:2182,172.27.22.112:2183";
    private static int SESSION_TIMEOUT = 140;

    private int count = 1000;

    ZooKeeper zooKeeper;
    private GReadWriteLock readWriteLock;
    private Lock writeLock;

    public Test(){
        try {
            zooKeeper = new ZooKeeper(HOSTS, SESSION_TIMEOUT, null);
            readWriteLock = new GReadWriteLock(zooKeeper, "lock1");
            writeLock = readWriteLock.writeLock();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        try{
            writeLock.lock();
            System.out.println(Thread.currentThread().getName() + " count " + count--);
            runInner();
        }finally {
            writeLock.unlock();
        }
    }

    private void runInner(){
        try{
            writeLock.lock();
            System.out.println(Thread.currentThread().getName() + " count " + count--);
        }finally {
            writeLock.unlock();
        }
    }

    public static void main(String[] orgs){
        final Test test = new Test();
        for(int i=0; i<500; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    test.run();
                }
            }).start();
        }
    }
}
