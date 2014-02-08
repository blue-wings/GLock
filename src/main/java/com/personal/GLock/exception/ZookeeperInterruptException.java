package com.personal.GLock.exception;

/**
 * User: FR
 * Time: 1/15/14 3:15 PM
 */
public class ZookeeperInterruptException extends RuntimeException {

    public ZookeeperInterruptException() {
    }

    public ZookeeperInterruptException(String message) {
        super(message);
    }

    public ZookeeperInterruptException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZookeeperInterruptException(Throwable cause) {
        super(cause);
    }

    public ZookeeperInterruptException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
