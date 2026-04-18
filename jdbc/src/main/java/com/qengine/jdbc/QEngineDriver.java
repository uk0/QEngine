package com.qengine.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver for QEngine (C time-series database, wire protocol v1).
 *
 * URL form:   jdbc:qengine://host:port/
 * Example:    jdbc:qengine://127.0.0.1:9000/
 *
 * Registered via java.sql.Driver SPI
 * (META-INF/services/java.sql.Driver).
 */
public class QEngineDriver implements Driver {

    public static final int MAJOR_VERSION = 0;
    public static final int MINOR_VERSION = 1;

    private static final String URL_PREFIX = "jdbc:qengine://";

    static {
        try {
            DriverManager.registerDriver(new QEngineDriver());
        } catch (SQLException e) {
            throw new RuntimeException("failed to register QEngineDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null; // JDBC contract: return null if URL not ours
        }
        String rest = url.substring(URL_PREFIX.length());
        // strip trailing '/' or path
        int slash = rest.indexOf('/');
        String hostPort = (slash >= 0) ? rest.substring(0, slash) : rest;
        if (hostPort.isEmpty()) {
            throw new QEngineWireException("URL missing host:port — " + url);
        }
        int colon = hostPort.lastIndexOf(':');
        String host;
        int port;
        if (colon < 0) {
            host = hostPort;
            port = 9000; // default
        } else {
            host = hostPort.substring(0, colon);
            try {
                port = Integer.parseInt(hostPort.substring(colon + 1));
            } catch (NumberFormatException nfe) {
                throw new QEngineWireException("invalid port in URL: " + url);
            }
        }
        String token = (info != null) ? info.getProperty("token", "") : "";
        String clientId = (info != null) ? info.getProperty("client_id", "qengine-jdbc") : "qengine-jdbc";
        int timeoutMs = 10_000;
        if (info != null) {
            String t = info.getProperty("timeout_ms");
            if (t != null) {
                try { timeoutMs = Integer.parseInt(t); } catch (NumberFormatException ignored) {}
            }
        }
        return new QEngineConnection(host, port, clientId, token, timeoutMs);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo token = new DriverPropertyInfo("token",
                info != null ? info.getProperty("token", "") : "");
        token.description = "Auth token forwarded in HELLO payload";
        token.required = false;

        DriverPropertyInfo cid = new DriverPropertyInfo("client_id",
                info != null ? info.getProperty("client_id", "qengine-jdbc") : "qengine-jdbc");
        cid.description = "Client identifier string sent in HELLO";
        cid.required = false;

        DriverPropertyInfo timeout = new DriverPropertyInfo("timeout_ms",
                info != null ? info.getProperty("timeout_ms", "10000") : "10000");
        timeout.description = "Socket SO_TIMEOUT in milliseconds";
        timeout.required = false;

        return new DriverPropertyInfo[] { token, cid, timeout };
    }

    @Override
    public int getMajorVersion() { return MAJOR_VERSION; }

    @Override
    public int getMinorVersion() { return MINOR_VERSION; }

    @Override
    public boolean jdbcCompliant() {
        // We do not implement the full JDBC spec (no transactions, no batch, etc.)
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("QEngineDriver: no java.util.logging parent logger");
    }
}
