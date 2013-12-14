import com.personal.GLock.GReadWriteLock;
import junit.framework.Assert;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Created with IntelliJ IDEA.
 * User: work
 * Date: 12/12/13
 * Time: 4:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class LockTest {

    private int count = 1000;

    ZooKeeper zooKeeper;
    private GReadWriteLock readWriteLock;
    private Lock writeLock;
    private Lock readLock;
    private Condition condition;

    public LockTest(){
        try {
            zooKeeper = new ZooKeeper(Config.HOSTS, Config.SESSION_TIMEOUT, null);
            readWriteLock = new GReadWriteLock(zooKeeper, "lock1");
            writeLock = readWriteLock.writeLock();
            readLock = readWriteLock.readLock();
            condition = writeLock.newCondition();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Integer testWriteLockIsolation(){
        try {
            writeLock.lock();
            return count--;
        }finally {
            writeLock.unlock();
        }
    }

    private boolean testReadLockShare(){
        try {
            long start = System.currentTimeMillis();
            readLock.lock();
            long duration = System.currentTimeMillis() - start;
            return duration>2;
        } finally {
            readLock.unlock();
        }
    }

    @Test
    public void writeLockIsolationTest() throws InterruptedException {
        final LockTest lockTest = new LockTest();
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        int threadNum = 50;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i = 0 ; i<threadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    concurrentHashMap.put(lockTest.testWriteLockIsolation(), "value");
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(950, lockTest.count);
    }

    @Test
    public void readLockShareTest() throws InterruptedException {
        final LockTest lockTest = new LockTest();
        int threadNum = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i = 0 ; i<threadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Assert.assertTrue(lockTest.testReadLockShare());
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
    }


}
