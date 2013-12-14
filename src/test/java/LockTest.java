import com.personal.GLock.GReadWriteLock;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
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

    public void writeRun(){
        try{
            if(writeLock.tryLock(3, TimeUnit.SECONDS)){
//                condition.await();
                System.out.println(Thread.currentThread().getName() + "write count " + count--);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            writeLock.unlock();
        }
    }

    @Test
    public void readRun(){
        try{
            if(readLock.tryLock(10, TimeUnit.SECONDS)){
                System.out.println(Thread.currentThread().getName() + "read count " + count);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            readLock.unlock();
        }
    }



    public static void main(String[] orgs){
        final LockTest test = new LockTest();
        for(int i=0; i<5; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    test.writeRun();
                }
            }).start();
        }
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
        for(int i=0; i<5; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    test.readRun();
                }
            }).start();
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
