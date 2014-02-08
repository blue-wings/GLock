package com.personal.GLock.state;

/**
 * User: FR
 * Time: 2/8/14 10:17 AM
 */
public enum ZookeeperState {
    ZOOKEEPER_KEEPER_ERROR,
    ZOOKEEPER_INTERRUPT,
    ZOOKEEPER_INTERRUPT_CREATE;

    public boolean isZookeeperError(){
        if(this == ZOOKEEPER_KEEPER_ERROR){
            return true;
        }
        return false;
    }


}
