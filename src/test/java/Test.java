import com.personal.GLock.GCondition;
import com.personal.GLock.GReadWriteLock;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Created with IntelliJ IDEA.
 * User: work
 * Date: 12/12/13
 * Time: 4:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    private static String HOSTS = "172.27.22.112:2181,172.27.22.112:2182,172.27.22.112:2183";
    private static int SESSION_TIMEOUT = 140;

    private int count = 1000;

    ZooKeeper zooKeeper;
    private GReadWriteLock readWriteLock;
    private Lock writeLock;
    private Lock readLock;
    private Condition condition;

    public Test(){
        try {
            zooKeeper = new ZooKeeper(HOSTS, SESSION_TIMEOUT, null);
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
            writeLock.lock();
            condition.await();
            System.out.println(Thread.currentThread().getName() + "write count " + count--);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            writeLock.unlock();
        }
    }

    public void readRun(){
        try{
            readLock.lock();
            System.out.println(Thread.currentThread().getName() + "read count " + count);
        }finally {
            readLock.unlock();
        }
    }



    public static void main(String[] orgs){
        final Test test = new Test();
        for(int i=0; i<5; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    test.writeRun();
                }
            }).start();
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        for(int i=0; i<1; i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    test.condition.signal();
                }
            }).start();
        }
    }
}
