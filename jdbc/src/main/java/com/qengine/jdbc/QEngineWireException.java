package com.qengine.jdbc;

import java.sql.SQLException;

/**
 * Exception raised for any failure in the QEngine wire protocol v1:
 * bad magic, CRC mismatch, unexpected message type, or server MSG_ERROR.
 */
public class QEngineWireException extends SQLException {
    private static final long serialVersionUID = 1L;

    public QEngineWireException(String message) {
        super(message);
    }

    public QEngineWireException(String message, Throwable cause) {
        super(message, cause);
    }

    public QEngineWireException(String message, int errCode) {
        super(message, null, errCode);
    }
}
