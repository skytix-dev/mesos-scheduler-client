package com.skytix.schedulerclient;

public class NoLeaderException extends Exception {

    public NoLeaderException() {
    }

    public NoLeaderException(String message) {
        super(message);
    }

    public NoLeaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoLeaderException(Throwable cause) {
        super(cause);
    }

    public NoLeaderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
