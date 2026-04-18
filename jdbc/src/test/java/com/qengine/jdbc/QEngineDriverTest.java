package com.qengine.jdbc;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the QEngine JDBC driver.
 *
 * Skipped (NOT failed) when no server is listening on localhost:9000.
 */
class QEngineDriverTest {

    private static final String HOST = "127.0.0.1";
    private static final int    PORT = 9000;
    private static final String URL  = "jdbc:qengine://" + HOST + ":" + PORT + "/";

    private static boolean canConnect() {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, PORT), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    @DisplayName("Driver is registered and accepts jdbc:qengine:// URLs")
    void driverAcceptsURL() throws Exception {
        // Triggers static init + SPI loading
        Class.forName("com.qengine.jdbc.QEngineDriver");

        Driver d = DriverManager.getDriver(URL);
        assertNotNull(d);
        assertTrue(d.acceptsURL(URL));
        assertEquals(false, d.acceptsURL("jdbc:mysql://host/db"));
        assertEquals(QEngineDriver.MAJOR_VERSION, d.getMajorVersion());
    }

    @Test
    @DisplayName("End-to-end SELECT — skipped if server is not running")
    void selectIntegration() throws Exception {
        Assumptions.assumeTrue(canConnect(),
                "Skipping: no QEngine server listening on " + HOST + ":" + PORT);

        Properties p = new Properties();
        p.setProperty("client_id", "qengine-jdbc-test");
        try (Connection c = DriverManager.getConnection(URL, p);
             Statement  s = c.createStatement();
             ResultSet  r = s.executeQuery("SELECT 1 AS n")) {

            ResultSetMetaData md = r.getMetaData();
            assertTrue(md.getColumnCount() >= 1, "at least 1 column");

            int rows = 0;
            while (r.next()) {
                // Accept int64 or float64 depending on how the server typed literal 1
                long v = r.getLong(1);
                assertEquals(1L, v, "SELECT 1 → 1");
                rows++;
            }
            assertTrue(rows >= 1, "at least one row");
        }
    }
}
