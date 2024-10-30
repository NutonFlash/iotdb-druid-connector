package org.kreps.druidtoiotdb.exceptions;

public class ServerErrorException extends Exception {
    private final int statusCode;

    public ServerErrorException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}