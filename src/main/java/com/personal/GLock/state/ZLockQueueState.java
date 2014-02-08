package com.personal.GLock.state;

/**
 * User: FR
 * Time: 2/7/14 2:51 PM
 */
public enum ZLockQueueState {

    OK,
    WAIT_INTERRUPT,
    WAKE_UP_FAILED,
    FAILED;

    public boolean isSuccess(){
        if(this == OK){
            return true;
        }
        return false;
    }

}
