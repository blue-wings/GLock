package com.personal.GLock.util;

import com.personal.GLock.state.ZookeeperState;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * User: FR
 * Time: 2/7/14 5:44 PM
 */
public class EnhancedZookeeperResult {
    private ZookeeperState state;
    private String node;
    private List<String> children;
    private String data;
    private Stat stat;
    private Boolean nodeIsDelete;

    public EnhancedZookeeperResult() {
    }

    public ZookeeperState getState() {
        return state;
    }

    public void setState(ZookeeperState state) {
        this.state = state;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public List<String> getChildren() {
        return children;
    }

    public void setChildren(List<String> children) {
        this.children = children;
    }

    public Stat getStat() {
        return stat;
    }

    public void setStat(Stat stat) {
        this.stat = stat;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Boolean getNodeIsDelete() {
        return nodeIsDelete==null?false:nodeIsDelete;
    }

    public void setNodeIsDelete(Boolean nodeIsDelete) {
        this.nodeIsDelete = nodeIsDelete;
    }

    public boolean isZookeeperError(){
        if(state!= null && state.isZookeeperError()){
            return true;
        }
        return false;
    }
}
