import com.personal.GLock.GReadWriteLock;
import junit.framework.Assert;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

    /**
     * purpose: test write lock synchronized
     * case:
     *  if: 50 threads
     *  action: get write lock to minus testUse.count
     *  result: testUser.count is 950
     * @throws InterruptedException
     */
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
                    concurrentHashMap.put(testUse.minusCountInWriteLock(), "value");
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(950, testUse.count);
    }

    /**
     * purpose: test read lock share
     * case:
     * if:10 threads
     * action: get read lock to read testUse.count
     * result:every thread get into zhe read lock
     * @throws InterruptedException
     */
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
                    boolean result = testUse.ifGetReadLockIn2Milliseconds();
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

    /**
     * purpose: test thread can get into try lock
     * case:
     * if:5 threads
     * action: thread try to get lock to minus testUse.count
     * result: at least one thread get in to lock minus testUse.count that testUse.count is not 1000 any more
     * @throws InterruptedException
     */
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
                    testUse.minusCountInTryLock();
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertNotSame(1000, testUse.count);
    }

    /**
     * purpose: test thread try lock failed
     * case;
     * if: 2 thread
     * action: two threads attempt get lock to minus testUse.count and sleep for a while
     * result: at least one thread try lock failed because may be test case previous zookeeper node dose not move yet.
     * @throws InterruptedException
     */
    @Test
    public void writeTryLockFailedTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int threadNum = 2;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i = 0 ; i<threadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.minusCountAndSleepInTryLock(2000);
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

    /**
     * purpose:
     */
    @Test
    public void readWaiteForWriteTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int readThreadNum = 5;
        final Set<Integer> readResult = new HashSet<Integer>();
        readResult.add(999);
        new Thread(new Runnable() {
            @Override
            public void run() {
                testUse.minusCountInWriteLock();
            }
        }).start();
        Thread.sleep(2000);
        for(int i=0; i<readThreadNum; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    readResult.add(testUse.getCountInReadLock());
                }
            }).start();
        }
        Assert.assertEquals(1, readResult.size());
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
        private Integer minusCountInWriteLock(){
            try {
                writeLock.lock();
                return count--;
            }finally {
                writeLock.unlock();
            }
        }

        private boolean ifGetReadLockIn2Milliseconds(){
            try {
                long start = System.currentTimeMillis();
                readLock.lock();
                long duration = System.currentTimeMillis() - start;
                return duration>2;
            } finally {
                readLock.unlock();
            }
        }

        private void minusCountInTryLock(){
            try {
                if(writeLock.tryLock()){
                    count--;
                }
            }finally {
                writeLock.unlock();
            }
        }

        private void minusCountAndSleepInTryLock(long milliSecond){
            try {
                if(writeLock.tryLock()){
                    count--;
                    try {
                        Thread.sleep(milliSecond);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }finally {
                writeLock.unlock();
            }
        }

        private int getCountInReadLock(){
            try {
               readLock.lock();
               return count;
            }finally {
                readLock.unlock();
            }
        }

        private void minusCountAndSleepInWriteLock(long milliseconds) throws InterruptedException {
            try {
                writeLock.lock();
                count--;
                 Thread.sleep(milliseconds);
            }finally {
                writeLock.unlock();
            }
        }


    }


}
