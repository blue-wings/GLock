package com.personal.GLock.exception;

/**
 * User: FR
 * Time: 1/15/14 3:15 PM
 */
public class GLockInterruptException extends RuntimeException {

    public GLockInterruptException() {
    }

    public GLockInterruptException(String message) {
        super(message);
    }

    public GLockInterruptException(String message, Throwable cause) {
        super(message, cause);
    }

    public GLockInterruptException(Throwable cause) {
        super(cause);
    }

    public GLockInterruptException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
