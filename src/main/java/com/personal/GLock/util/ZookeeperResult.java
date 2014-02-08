package com.personal.GLock.util;

import com.personal.GLock.state.ZLockQueueState;
import com.personal.GLock.state.ZookeeperState;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * User: FR
 * Time: 2/7/14 5:44 PM
 */
public class ZookeeperResult {
    private ZookeeperState state;
    private String node;
    private List<String> children;
    private byte[] data;
    private Stat stat;

    public ZookeeperResult() {
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

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public Stat getStat() {
        return stat;
    }

    public void setStat(Stat stat) {
        this.stat = stat;
    }
}
