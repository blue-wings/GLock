package com.personal.GLock.util;

import com.personal.GLock.state.ZookeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * this class ensure zookeeper execution can't be interrupted
 * unless zookeeper has own error, process must execute successfully
 * User: FR
 * Time: 2/7/14 4:33 PM
 */
public class EnhancedZookeeper {

    private static Logger logger = LoggerFactory.getLogger(EnhancedZookeeper.class);

    private static final String WRAP_DATA_SPLITTER ="-";

    public static EnhancedZookeeperResult deleteNode(ZooKeeper zooKeeper, String path, int version) {
        EnhancedZookeeperResult enhancedZookeeperResult = new EnhancedZookeeperResult();
        try {
            zooKeeper.delete(path, version);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            enhancedZookeeperResult.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            return deleteNode(zooKeeper, path, version);
        }
        return enhancedZookeeperResult;
    }

    public static EnhancedZookeeperResult exist(ZooKeeper zooKeeper, String path, Watcher watch){
        EnhancedZookeeperResult result = new EnhancedZookeeperResult();
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

    public static EnhancedZookeeperResult exist(ZooKeeper zooKeeper, String path, boolean watch){
        EnhancedZookeeperResult result = new EnhancedZookeeperResult();
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

    public static EnhancedZookeeperResult create(ZooKeeper zooKeeper, String createPath, String superPath, String data, List<ACL> acl, CreateMode createMode){
        String dataKey = Config.CLIENT_ID+UUID.randomUUID().toString().replaceAll("-","");
        String wrapData = dataKey + WRAP_DATA_SPLITTER + data;
        EnhancedZookeeperResult result = new EnhancedZookeeperResult();
        String preNode = null;
        try {
            EnhancedZookeeperResult getChildrenResult = getChildren(zooKeeper, superPath, false);
            List<String> children = getChildrenResult.getChildren();
            if(children!=null && !children.isEmpty()){
                Collections.sort(children);
                preNode = children.get(children.size()-1);
            }
            String node = zooKeeper.create(createPath, wrapData.getBytes(), acl, createMode);
            result.setNode(node);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            result.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            ZookeeperState delCreateNode = delCreatedNode(zooKeeper, superPath, preNode, dataKey);
            if(delCreateNode.isZookeeperError()){
                result.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
                return result;
            }
            return create(zooKeeper, createPath, superPath, data, acl, createMode);
        }
        return result;
    }

    private static ZookeeperState delCreatedNode(ZooKeeper zooKeeper, String superPath, String preNode, String dataKey){
        EnhancedZookeeperResult getChildrenResult = getChildren(zooKeeper, superPath, false);
        if(getChildrenResult.isZookeeperError()){
            return ZookeeperState.ZOOKEEPER_KEEPER_ERROR;
        }
        List<String> children = getChildrenResult.getChildren();
        Collections.sort(children);
        if(preNode == null){
            String createdNode = children.get(0);
            EnhancedZookeeperResult delResult = deleteNode(zooKeeper, superPath+Config.SPLITTER +createdNode, -1);
            if(delResult.isZookeeperError()){
                return ZookeeperState.ZOOKEEPER_KEEPER_ERROR;
            }
        }else {
            int preNodeIndex = children.indexOf(preNode);
            for(int i = preNodeIndex+1; i<children.size(); i++){
                String node = children.get(i);
                EnhancedZookeeperResult getDataResult = getData(zooKeeper, superPath+Config.SPLITTER +node, false, null, true);
                if(getDataResult.isZookeeperError()){
                    EnhancedZookeeperResult existResult = exist(zooKeeper, superPath+Config.SPLITTER +node, false);
                    if(existResult.isZookeeperError()){
                        return ZookeeperState.ZOOKEEPER_KEEPER_ERROR;
                    }
                    if(getChildrenResult.getStat() == null){
                        continue;
                    }
                }
                String wrappedData = getDataResult.getData();
                String key = wrappedData.substring(0, wrappedData.indexOf(WRAP_DATA_SPLITTER));
                if(key.equals(dataKey)){
                    EnhancedZookeeperResult delNodeResult = deleteNode(zooKeeper, superPath+Config.SPLITTER +node, -1);
                    if(delNodeResult.isZookeeperError()){
                        return ZookeeperState.ZOOKEEPER_KEEPER_ERROR;
                    }
                }
            }
        }
        return ZookeeperState.ZOOKEEPER_OK;
    }

    public static EnhancedZookeeperResult getChildren(ZooKeeper zooKeeper, String path, boolean watch){
        EnhancedZookeeperResult result = new EnhancedZookeeperResult();
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

    public static EnhancedZookeeperResult getData(ZooKeeper zooKeeper, String path, boolean watch, Stat stat, boolean wrapped){
        EnhancedZookeeperResult result = new EnhancedZookeeperResult();
        try {
            byte[] data = zooKeeper.getData(path, watch, stat);
            String wrapData = new String(data);
            if(wrapped){
                result.setData(wrapData);
            }else {
                String realData = wrapData==null?null:wrapData.substring(wrapData.indexOf(WRAP_DATA_SPLITTER)+1);
                result.setData(realData);
            }
        } catch (KeeperException e) {
            EnhancedZookeeperResult exitResult = exist(zooKeeper, path, false);
            if(!exitResult.isZookeeperError() && exitResult.getStat()==null){
                logger.debug(path+" is deleted yet");
                result.setNodeIsDelete(true);
            }else {
                logger.error(e.getMessage());
                result.setState(ZookeeperState.ZOOKEEPER_KEEPER_ERROR);
            }
        } catch (InterruptedException e) {
            logger.debug("zookeeper can't be interrupted");
            return getData(zooKeeper, path, watch, stat, wrapped);
        }
        return result;
    }

}
