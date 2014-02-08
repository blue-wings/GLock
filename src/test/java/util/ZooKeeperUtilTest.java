package util;

import com.personal.GLock.state.ZookeeperState;
import com.personal.GLock.util.ZookeeperResult;
import com.personal.GLock.util.ZookeeperUtil;
import junit.framework.Assert;
import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;

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
        ZookeeperResult result = ZookeeperUtil.exist(zooKeeper, "/notExistNode", null);
        Assert.assertNull(result.getStat());
    }

    /**
     * get data throw Exception
     */
    @Test
    public void testGetDataOfNotExistNode(){
        ZookeeperResult result = ZookeeperUtil.getData(zooKeeper, "/notExistNode", false, null);
        Assert.assertEquals(ZookeeperState.ZOOKEEPER_KEEPER_ERROR, result.getState());
    }

    @Test
    public void testExistWatchTwice(){
        ZookeeperResult result = ZookeeperUtil.create(zooKeeper, "/test/testExistWatchTwice", "aa".getBytes(), null, CreateMode.EPHEMERAL_SEQUENTIAL);
//        ZookeeperUtil.exist(zooKeeper, result.getNode(), new TestWatcher());
//        ZookeeperUtil.exist(zooKeeper, result.getNode(), new TestWatcher());
//        ZookeeperUtil.deleteNode(zooKeeper, result.getNode(), -1);
    }

    private class TestWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            System.out.println("execute watch");
        }
    }



}
