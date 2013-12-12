package com.personal.GLock;

/**
 * User: FR
 * Time: 13-12-12 下午4:31
 */
public class PathIndex {
    public static final String LOCK_ROOT_PATH = "/zLock";
    public static final String READ_LOCK_NODE_PATH = LOCK_ROOT_PATH+"/readLock";
    public static final String WRITE_LOCK_NODE_PATH = LOCK_ROOT_PATH+"/writeLock";
    public static final String WAITING_QUEUE_NODE_PATH = LOCK_ROOT_PATH+"/waitingQueue";
    public static final String WAKEUP_QUEUE_NODE_PATH=LOCK_ROOT_PATH+"/wakeupQueue";
    public static final String SPLITER = "/";
}
