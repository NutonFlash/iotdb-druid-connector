package org.kreps.druidtoiotdb.exceptions;

public class ClientErrorException extends RuntimeException {
    private final int statusCode;

    public ClientErrorException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}