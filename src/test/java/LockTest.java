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

    @Test
    public void writeLockIsolationTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        int threadNum = 50;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i = 0 ; i<threadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    concurrentHashMap.put(testUse.testWriteLockIsolation(), "value");
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(950, testUse.count);
    }

    @Test
    public void readLockShareTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int threadNum = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        for(int i = 0 ; i<threadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    boolean result = testUse.testReadLockShare();
                    Assert.assertTrue(result);
                    if(result){
                        concurrentHashMap.put(Thread.currentThread().getName(), "value");
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(threadNum, concurrentHashMap.size());
    }

    @Test
    public void writeTryLockSuccessTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        int threadNum = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i = 0 ; i<threadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.testWriteTryLockSuccess();
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertNotSame(1000, testUse.count);
    }

    @Test
    public void writeTryLockFailedTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        int threadNum = 2;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i = 0 ; i<threadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.testWriteTryLockSuccess();
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        boolean result = false;
        if(testUse.count==1000 || testUse.count==999){
            result = true;
        }
        Assert.assertTrue(result);
    }

    private class TestUse{
        private int count = 1000;

        ZooKeeper zooKeeper;
        private GReadWriteLock readWriteLock;
        private Lock writeLock;
        private Lock readLock;
        private Condition condition;

        public TestUse(){
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

        private void testWriteTryLockSuccess(){
            try {
                if(writeLock.tryLock()){
                    count--;
                }
            }finally {
                writeLock.unlock();
            }
        }
    }


}
