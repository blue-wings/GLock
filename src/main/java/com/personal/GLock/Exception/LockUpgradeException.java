package com.personal.GLock.Exception;

/**
 * User: FR
 * Time: 12/13/13 10:22 AM
 */
public class LockUpgradeException extends RuntimeException{
    public LockUpgradeException() {
    }

    public LockUpgradeException(String message) {
        super(message);
    }

    public LockUpgradeException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockUpgradeException(Throwable cause) {
        super(cause);
    }

    public LockUpgradeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
