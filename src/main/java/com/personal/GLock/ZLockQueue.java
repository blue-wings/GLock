package com.personal.GLock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * User: FR
 * Time: 13-12-12 下午4:34
 */
public class ZLockQueue {
    private ZooKeeper zooKeeper;
    private String lockKey;
    private boolean isWriteLock;

    private String node;
    private String preNode;

    private int lockTimes;

    ZLockQueue(ZooKeeper zooKeeper, String lockKey, boolean writeLock) {
        this.zooKeeper = zooKeeper;
        this.lockKey = lockKey;
        isWriteLock = writeLock;
        createZNode();
    }

    synchronized boolean getMyTurn(boolean forWait, long time, TimeUnit unit){
        try {
            printThreadNode("get");
            List<String> children = zooKeeper.getChildren(PathIndex.WRITE_LOCK_NODE_PATH + "/" + lockKey, false);
            Collections.sort(children);
            int index = Collections.binarySearch(children, node);
            if(index== 0){
                return true;
            }else{
                preNode = children.get(index-1);
            }
            printThreadNode("get-pre");
            Stat stat = zooKeeper.exists(PathIndex.WRITE_LOCK_NODE_PATH + "/" + lockKey+"/"+preNode, new preNodeDelWatcher());
            if(stat == null){
                return getMyTurn(forWait, time, unit);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(forWait){
            try {
                if(unit == null){
                    wait();
                }else {
                    wait(unit.toMillis(time/10000), (int)unit.toNanos(time%10000));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            getMyTurn(forWait, time, unit);
        }
        return false;
    }

    void remove(){
        try {
            if(zooKeeper.exists(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+ PathIndex.SPLITER+node, false)!=null){
                zooKeeper.delete(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+ PathIndex.SPLITER+node, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private class preNodeDelWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            synchronized (ZLockQueue.this){
                printThreadNode("preDelete");
                ZLockQueue.this.notify();
            }
        }
    }

    private void createZNode() {
        try {
            if (isWriteLock) {
                node = zooKeeper.create(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+ PathIndex.SPLITER, Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } else if (!isWriteLock) {
                node = zooKeeper.create(PathIndex.READ_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+ PathIndex.SPLITER, Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } else {
                throw new RuntimeException("lock type  ambiguity");
            }
            node = node.substring(node.lastIndexOf(PathIndex.SPLITER)+1);
            printThreadNode("create");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void printThreadNode(String msgHead) {
        System.out.println(msgHead+"-"+Thread.currentThread().getName()+"-"+ toString());
    }

    @Override
    public String toString() {
        return "ZLock{" +
                "node='" + node + '\'' +
                ", preNode='" + preNode + '\'' +
                '}';
    }

    int lockTimesInc(){
        return ++lockTimes;
    }

    int lockTimesDec(){
        return --lockTimes;
    }
}
