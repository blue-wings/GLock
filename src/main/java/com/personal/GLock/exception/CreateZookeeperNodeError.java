package com.personal.GLock.exception;

/**
 * User: FR
 * Time: 2/7/14 6:02 PM
 */
public class CreateZookeeperNodeError extends Error {

    public CreateZookeeperNodeError() {
    }

    public CreateZookeeperNodeError(String message) {
        super(message);
    }

    public CreateZookeeperNodeError(String message, Throwable cause) {
        super(message, cause);
    }

    public CreateZookeeperNodeError(Throwable cause) {
        super(cause);
    }

    public CreateZookeeperNodeError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
