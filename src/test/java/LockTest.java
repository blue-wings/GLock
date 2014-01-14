import com.personal.GLock.GReadWriteLock;
import junit.framework.Assert;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    /**
     * purpose: test write lock synchronized
     * case:
     * if: 50 threads
     * action: get write lock to minus testUse.count
     * result: testUser.count is 950
     *
     * @throws InterruptedException
     */
    @Test
    public void writeLockIsolationTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        int threadNum = 50;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
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
     *
     * @throws InterruptedException
     */
    @Test
    public void readLockShareTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int threadNum = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        for (int i = 0; i < threadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    boolean result = testUse.ifGetReadLockIn2Milliseconds();
                    Assert.assertTrue(result);
                    if (result) {
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
     *
     * @throws InterruptedException
     */
    @Test
    public void writeTryLockSuccessTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final ConcurrentHashMap concurrentHashMap = new ConcurrentHashMap();
        int threadNum = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.minusCountInWriteTryLock();
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertNotSame(1000, testUse.count);
    }

    /**
     * purpose: test thread try lock failed
     * case
     * if: 2 thread
     * action: two threads attempt get lock to minus testUse.count and sleep for a while
     * result: at least one thread try lock failed because may be test case previous zookeeper node dose not move yet.
     *
     * @throws InterruptedException
     */
    @Test
    public void writeTryLockFailedTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int threadNum = 2;
        final CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.minusCountAndSleepInWriteTryLock(2000);
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        boolean result = false;
        if (testUse.count == 1000 || testUse.count == 999) {
            result = true;
        }
        Assert.assertTrue(result);
    }

    /**
     * purpose: test read lock must after write lock release
     * case
     * if: one write thread, five read thread
     * action: one thread get write lock to minus count, and five threads get read lock  to read count
     * result: read lock and write lock does not mixed, all read thread get zhe same result.
     */
    @Test
    public void readWaiteForWriteTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int readThreadNum = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(6);
        final Set<Integer> readResult = new HashSet<Integer>();
        readResult.add(999);
        new Thread(new Runnable() {
            @Override
            public void run() {
                testUse.minusCountInWriteLock();
                countDownLatch.countDown();
            }
        }).start();
        Thread.sleep(2000);
        for (int i = 0; i < readThreadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    readResult.add(testUse.getCountInReadLock());
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(1, readResult.size());
    }

    /**
     * purpose: write lock muster after read lock release
     * if: five read thread, five write thread
     * action: five read thread get read lock and five write thread get write lock to write count
     * result; read thread read the same count, write lock must waite for read lock release
     * @throws InterruptedException
     */
    @Test
    public void writeWaitForReadTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int readThreadNum = 5;
        int writeThreadNum = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        final Set<Integer> readResult = new HashSet<Integer>();
        readResult.add(1000);
        for (int i = 0; i < readThreadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        readResult.add(testUse.getCountAndSleepInReadLock(500));
                        countDownLatch.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        Thread.sleep(2000);
        for (int i = 0; i < writeThreadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.minusCountInWriteLock();
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(1, readResult.size());
        Assert.assertEquals(995, testUse.count);
    }

    /**
     * purpose: read, write thread mixed to operate count
     * if: five read thread, five write thread
     * action: ten thread concurrent to operate count
     * result: five write thread minus right
     * @throws InterruptedException
     */
    @Test
    public void readWriteLockMixedTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int readThreadNum = 5;
        int writeThreadNum = 5;
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i = 0; i < readThreadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.getCountInReadLock();
                    countDownLatch.countDown();
                }
            }).start();
        }
        for (int i = 0; i < writeThreadNum; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testUse.minusCountInWriteLock();
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(995, testUse.count);
    }

    /**
     *  purpose: try write lock success
     *  if: one thread
     *  action: try write lock in 2sec
     *  result: success
     */
    @Test
    public void writeTryWriteLockTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int readThreadNum = 1;
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    testUse.minusCountInWriteTryLock(2000);
                    countDownLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        countDownLatch.await();
        Assert.assertEquals(999, testUse.count);
    }

    /**
     *  purpose: try read lock success
     *  if: one thread
     *  action: try read lock in 2sec
     *  result: success
     */
    @Test
    public void writeTryReadLockTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        int readThreadNum = 1;
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final List<Integer> result = new ArrayList<Integer>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    result.add(testUse.getCountInReadTryLock(2000));
                    countDownLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        countDownLatch.await();
        Assert.assertEquals(1000, result.get(0).intValue());
    }

    /**
     * purpose: test try lock wait timeout
     * if: one write thread get lock , then the other thread want to get lock  in that second
     * result: try lock time out, get lock failed
     * @throws InterruptedException
     */
    @Test
    public void readTryLockInTimeFailedTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        final List<Integer> result = new ArrayList<Integer>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    testUse.minusCountAndSleepInWriteLock(2000);
                    countDownLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        Thread.sleep(10);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Integer r = testUse.getCountInReadTryLock(1000);
                    if(r !=null){
                        result.add(r);
                    }
                    countDownLatch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        countDownLatch.await();
        Assert.assertEquals(0, result.size());
    }

    /**
     * purpose: test read try lock share
     * if: five thread
     * action: try read lock in 2esc
     * result; all success
     */
    @Test
    public void   readTryLockShareTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final CountDownLatch countDownLatch = new CountDownLatch(5);
        final List<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Integer r = testUse.getCountInReadTryLock(2000);
                        if(r!=null){
                            result.add(r);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        Assert.assertEquals(5, result.size());
    }

    @Test
    public void writeLockInterruptTest() throws InterruptedException {
        final TestUse testUse = new TestUse();
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    testUse.getCountAndSleepInReadLock(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            }
        });
        Thread interruptThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    testUse.minusCountInWriteLock();
                } finally {
                    countDownLatch.countDown();
                }
            }
        });
        thread.start();
        Thread.sleep(10);
        interruptThread.start();
        Thread.sleep(1000);
        interruptThread.interrupt();
        countDownLatch.await();
        Assert.assertEquals(999, testUse.count);
    }

    private class TestUse {
        private int count = 1000;

        ZooKeeper zooKeeper;
        private GReadWriteLock readWriteLock;
        private Lock writeLock;
        private Lock readLock;
        private Condition condition;

        public TestUse() {
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

        private Integer minusCountInWriteLock() {
            try {
                writeLock.lock();
                return count--;
            } finally {
                writeLock.unlock();
            }
        }

        private boolean ifGetReadLockIn2Milliseconds() {
            try {
                long start = System.currentTimeMillis();
                readLock.lock();
                long duration = System.currentTimeMillis() - start;
                return duration > 2;
            } finally {
                readLock.unlock();
            }
        }

        private void minusCountInWriteTryLock() {
            try {
                if (writeLock.tryLock()) {
                    count--;
                }
            } finally {
                writeLock.unlock();
            }
        }

        private void minusCountInWriteTryLock(long millisecond) throws InterruptedException {
            try {
                if (writeLock.tryLock(millisecond, TimeUnit.MILLISECONDS)) {
                    count--;
                }
            } finally {
                writeLock.unlock();
            }
        }

        private void minusCountAndSleepInWriteTryLock(long millisecond) {
            try {
                if (writeLock.tryLock()) {
                    count--;
                    try {
                        Thread.sleep(millisecond);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }

        private int getCountInReadLock() {
            try {
                readLock.lock();
                return count;
            } finally {
                readLock.unlock();
            }
        }

        private void minusCountAndSleepInWriteLock(long milliseconds) throws InterruptedException {
            try {
                writeLock.lock();
                count--;
                Thread.sleep(milliseconds);
            } finally {
                writeLock.unlock();
            }
        }

        private int getCountAndSleepInReadLock(long milliseconds) throws InterruptedException {
            try {
                readLock.lock();
                Thread.sleep(milliseconds);
                return count;
            } finally {
                readLock.unlock();
            }
        }

        private Integer getCountInReadTryLock(long milliseconds) throws InterruptedException {
            try {
                if(readLock.tryLock(milliseconds, TimeUnit.MILLISECONDS)){
                    return count;
                }
            } finally {
                readLock.unlock();
            }
            return null;
        }


    }


}
