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
    private final ZooKeeper zooKeeper;
    private final String lockKey;
    private final boolean isWriteLock;

    private String node;
    private String preNode;

    private int lockTimes;
    private boolean startByWakeUp = false;
    private boolean maintainNode=false;

    ZLockQueue(ZooKeeper zooKeeper, String lockKey, boolean writeLock) {
        this.zooKeeper = zooKeeper;
        this.lockKey = lockKey;
        isWriteLock = writeLock;
        createZNode();
    }

    synchronized boolean getMyTurn(boolean forWait, long time, TimeUnit unit){
        if(isWriteLock){
            return getWriteMyTurn(forWait, time, unit);
        }
        return getReadMyTurn(forWait, time, unit);
    }

    private boolean getWriteMyTurn(boolean forWait, long time, TimeUnit unit){
        try {
            List<String> children = zooKeeper.getChildren(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey, false);
            Collections.sort(children);
            int index = Collections.binarySearch(children, node);
            if(index== 0){
                System.out.println(Thread.currentThread().getName()+" get write lock "+toString());
                maintainNode = false;
                return true;
            }else if(startByWakeUp){
                System.out.println(Thread.currentThread().getName()+"try write lock expired");
                maintainNode=true;
                return false;
            }else{
                preNode = children.get(index-1);
            }
            Stat stat = zooKeeper.exists(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+PathIndex.SPLITER+preNode, new preNodeDelWatcher());
            if(stat == null){
                return getWriteMyTurn(forWait, time, unit);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(forWait){
            try {
                if(time == Long.MAX_VALUE){
                    wait();
                }else {
                    wait(unit.toMillis(time));
                }
                System.out.println(Thread.currentThread().getName()+"write wake up");
                startByWakeUp = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return getWriteMyTurn(forWait, time, unit);
        }
        return false;
    }

    private boolean getReadMyTurn(boolean forWait, long time, TimeUnit unit){
        try {
            List<String> children = zooKeeper.getChildren(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER+ lockKey, false);
            Collections.sort(children);
            int index = Collections.binarySearch(children, node);
            if(index == 0){
                System.out.println(Thread.currentThread().getName()+" get read lock "+toString());
                return true;
            }
            for(int i=index-1; i>=0; i-- ){
                String pre = children.get(i);
                try{
                    byte[] data = zooKeeper.getData(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+PathIndex.SPLITER+pre, false, null);
                    if(data!=null && new String(data).equals(PathIndex.WRITE_NODE_DATA)){
                        preNode = pre;
                        break;
                    }
                } catch (KeeperException e){
                    continue;
                }
            }
            if(preNode==null){
                System.out.println(Thread.currentThread().getName()+" get read lock "+toString());
                return true;
            }else if(startByWakeUp){
                System.out.println(Thread.currentThread().getName()+"pre node is" + toString());
                Stat stat = zooKeeper.exists(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+PathIndex.SPLITER+preNode, new preNodeDelWatcher());
                if(stat == null){
                    System.out.println(Thread.currentThread().getName()+" get read lock "+toString());
                    return true;
                }
                System.out.println(Thread.currentThread().getName()+"try read lock expired");
                maintainNode=true;
                return false;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(forWait){
            try {
                if(time == Long.MAX_VALUE){
                    wait();
                }else {
                    wait(unit.toMillis(time));
                }
                System.out.println(Thread.currentThread().getName()+"read wake up");
                startByWakeUp = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return getReadMyTurn(forWait, time, unit);
        }
        return false;
    }

    void remove(){
        try {
            if( !maintainNode && zooKeeper.exists(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey+ PathIndex.SPLITER+node, false)!=null){
                zooKeeper.delete(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER + node, -1);
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
                if(maintainNode){
                    maintainNode=false;
                    remove();
                } else {
                    ZLockQueue.this.notify();
                }
            }
        }
    }

    void reInQueue(boolean forWait, long time, TimeUnit unit){
        createZNode();
        getMyTurn(forWait, time, unit);
    }

    private void createZNode() {
        try {
            String data = isWriteLock?PathIndex.WRITE_NODE_DATA:PathIndex.READ_NODE_DATA;
            node = zooKeeper.create(PathIndex.WRITE_LOCK_NODE_PATH + PathIndex.SPLITER + lockKey + PathIndex.SPLITER, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            node = node.substring(node.lastIndexOf(PathIndex.SPLITER)+1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    int lockTimesInc(){
        return ++lockTimes;
    }

    int lockTimesDec(){
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
        return Thread.currentThread().getName()+"  ZLockQueue{" +
                "isWriteLock=" + isWriteLock +
                ", node='" + node + '\'' +
                ", preNode='" + preNode + '\'' +
                ", lockTimes=" + lockTimes +
                '}';
    }
}
