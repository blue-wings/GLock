package com.personal.GLock.util;

import com.personal.GLock.exception.CreateZookeeperNodeError;
import com.personal.GLock.state.ZookeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * User: FR
 * Time: 2/7/14 4:33 PM
 */
public class ZookeeperUtil {

    private static Logger logger = LoggerFactory.getLogger(ZookeeperUtil.class);

    public static ZookeeperResult deleteNode(ZooKeeper zooKeeper, String path, int version) {
        ZookeeperResult zookeeperResult = new ZookeeperResult();
        try {
            zooKeeper.delete(path, version);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            zookeeperResult.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            return deleteNode(zooKeeper, path, version);
        }
        return zookeeperResult;
    }

    public static ZookeeperResult exist(ZooKeeper zooKeeper, String path, Watcher watch){
        ZookeeperResult result = new ZookeeperResult();
        try {
            Stat stat = zooKeeper.exists(path, watch);
            result.setStat(stat);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            result.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            return exist(zooKeeper, path, watch);
        }
        return result;
    }

    public static ZookeeperResult create(ZooKeeper zooKeeper, String path, byte data[], List<ACL> acl, CreateMode createMode){
        ZookeeperResult result = new ZookeeperResult();
        try {
            String node = zooKeeper.create(path, data, acl, createMode);
            result.setNode(node);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            result.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            throw new CreateZookeeperNodeError("create zookeeper node interrupted, it is fetal error");
        }
        return result;
    }

    public static ZookeeperResult getChildren(ZooKeeper zooKeeper, String path, boolean watch){
        ZookeeperResult result = new ZookeeperResult();
        try {
            List<String> children = zooKeeper.getChildren(path, watch);
            result.setChildren(children);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            result.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            return getChildren(zooKeeper, path, watch);
        }
        return result;
    }

    public static ZookeeperResult getData(ZooKeeper zooKeeper, String path, boolean watch, Stat stat){
        ZookeeperResult result = new ZookeeperResult();
        try {
            byte[] data = zooKeeper.getData(path, watch, stat);
            result.setData(data);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            result.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            return getData(zooKeeper, path, watch, stat);
        }
        return result;
    }

}
