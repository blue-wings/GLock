package com.personal.GLock.util;

/**
 * User: FR
 * Time: 2/8/14 10:57 AM
 */
public class Config {

    //path
    public static final String SPLITER = "/";
    public static final String LOCK_ROOT_PATH = "/zLock";
    public static final String WAKEUP_QUEUE_NODE_PATH=LOCK_ROOT_PATH+"/wakeupQueue";
    public static final String WAITING_QUEUE_NODE_PATH = LOCK_ROOT_PATH+"/waitingQueue";
    public static final String WRITE_LOCK_NODE_PATH = LOCK_ROOT_PATH+"/writeLock";
    public static final String READ_LOCK_NODE_PATH = LOCK_ROOT_PATH+"/readLock";

    //data
    public static final String WRITE_NODE_DATA="write";
    public static final String READ_NODE_DATA="read";
    public static final String WAIT_NODE_DATA="wait";

    //logic control
    private Boolean lockSwitch;
    public Boolean getLockSwitch() {
        return lockSwitch;
    }

    public void setLockSwitch(Boolean lockSwitch) {
        this.lockSwitch = lockSwitch;
    }
}
