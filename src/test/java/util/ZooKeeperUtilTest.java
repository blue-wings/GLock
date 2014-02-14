package util;

import com.personal.GLock.state.ZookeeperState;
import com.personal.GLock.util.EnhancedZookeeper;
import com.personal.GLock.util.EnhancedZookeeperResult;
import junit.framework.Assert;
import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: FR
 * Time: 2/8/14 2:30 PM
 */
public class ZooKeeperUtilTest {

    ZooKeeper zooKeeper;

    public ZooKeeperUtilTest() throws IOException {
        zooKeeper = new ZooKeeper(Config.HOSTS, Config.SESSION_TIMEOUT, null);
    }

    /**
     * zookeeper not exist not throw exception
     */
    @Test
    public void testNodeNotExist(){
        EnhancedZookeeperResult result = EnhancedZookeeper.exist(zooKeeper, "/test/notExistNode", null);
        Assert.assertNull(result.getStat());
    }

    /**
     * get data throw Exception
     */
    @Test
    public void testGetDataOfNotExistNode(){
        EnhancedZookeeperResult result = EnhancedZookeeper.getData(zooKeeper, "/test/notExistNode", false, null, false);
        Assert.assertEquals(ZookeeperState.ZOOKEEPER_KEEPER_ERROR, result.getState());
    }

    /**
     * bind watch twice
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testExistWatchTwice() throws IOException, InterruptedException {
        EnhancedZookeeperResult result = EnhancedZookeeper.create(zooKeeper, "/test", "/test/testExistWatchTwice", "test", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        AtomicInteger count = new AtomicInteger(2);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        EnhancedZookeeper.exist(zooKeeper, result.getNode(), new TestWatcher(count, countDownLatch));
        EnhancedZookeeper.exist(zooKeeper, result.getNode(), new TestWatcher(count, countDownLatch));
        EnhancedZookeeper.deleteNode(zooKeeper, result.getNode(), -1);
        countDownLatch.await();
        Assert.assertEquals(0, count);
    }

    private class TestWatcher implements Watcher {
        AtomicInteger count;
        CountDownLatch countDownLatch;

        private TestWatcher(AtomicInteger count, CountDownLatch countDownLatch) {
            this.count = count;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void process(WatchedEvent event) {
            count.decrementAndGet();
            countDownLatch.countDown();
            System.out.println("execute watch");
        }
    }



}
