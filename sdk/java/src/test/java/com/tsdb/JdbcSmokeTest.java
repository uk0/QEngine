package com.tsdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Requires a live tsdb-server on {@code -Dtsdb.addr=host:port}
 * (default 127.0.0.1:28090) with require_auth=false.  Not a JUnit test —
 * runnable as a standalone main so it works without pulling in JUnit.
 */
public class JdbcSmokeTest {
    public static void main(String[] args) throws Exception {
        String addr = System.getProperty("tsdb.addr", "127.0.0.1:28090");
        // Force class load → Driver registers itself.
        Class.forName("com.tsdb.jdbc.TsdbDriver");

        String url = "jdbc:tsdb://" + addr;
        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement st = c.createStatement()) {
                // DDL
                try { st.executeUpdate("DROP TABLE jdbc_smoke"); } catch (Exception ignore) {}
                st.executeUpdate("CREATE TABLE jdbc_smoke (ts TIMESTAMP, v INT64) TIMESTAMP(ts)");
                // Read path
                try (ResultSet r = st.executeQuery("SELECT count(*) FROM jdbc_smoke")) {
                    if (!r.next()) throw new AssertionError("no row");
                    long n = r.getLong(1);
                    System.out.println("JDBC: count(*) = " + n);
                    if (n < 0) throw new AssertionError("bad count");
                }
            }
        }
        System.out.println("JDBC smoke OK");
    }
}
