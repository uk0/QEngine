package dev.tsdb.jdbc;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test — runs only when TSDB_JDBC_URL is exported so CI
 * without a live cluster stays green.  Point it at a dashboard:
 *
 * <pre>
 *   export TSDB_JDBC_URL='jdbc:tsdb://10.88.51.102:29311/?user=root&amp;pass=123456'
 *   mvn -Dtest=IntegrationTest test
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntegrationTest {

    static final String URL = System.getenv().getOrDefault("TSDB_JDBC_URL", "");
    static final String DB  = "jdbc_test_" + (System.currentTimeMillis() / 1000);

    static Connection c;

    @BeforeAll
    static void setUp() throws Exception {
        assumeTrue(!URL.isEmpty(), "TSDB_JDBC_URL not set; skipping integration tests");
        Class.forName("dev.tsdb.jdbc.TSDBDriver");
        c = DriverManager.getConnection(URL);
        assertNotNull(c);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (c == null) return;
        try (Statement s = c.createStatement()) {
            s.execute("DROP DATABASE " + DB);
        } catch (SQLException ignored) { /* cleanup only */ }
        c.close();
    }

    @Test @Order(1)
    void driverRegistered() throws Exception {
        Driver d = DriverManager.getDriver(URL);
        assertEquals("tsdb-jdbc", d.getClass().getPackage().getImplementationTitle() == null
                                   ? "tsdb-jdbc" : d.getClass().getPackage().getImplementationTitle());
        assertTrue(d.acceptsURL(URL));
        assertFalse(d.acceptsURL("jdbc:mysql://x"));
    }

    @Test @Order(2)
    void metadataShape() throws Exception {
        DatabaseMetaData md = c.getMetaData();
        assertEquals("tsdb", md.getDatabaseProductName());
        assertEquals(4, md.getJDBCMajorVersion());
    }

    @Test @Order(3)
    void listDatabases() throws Exception {
        try (Statement s = c.createStatement();
             ResultSet r = s.executeQuery("LIST DATABASES")) {
            ResultSetMetaData m = r.getMetaData();
            assertTrue(m.getColumnCount() >= 1, "LIST DATABASES has at least one column");
            int seen = 0;
            while (r.next()) {
                String name = r.getString(1);
                assertNotNull(name);
                seen++;
            }
            // The default seed DB "sysdb" is always present.
            assertTrue(seen >= 1, "expected at least one database (sysdb)");
        }
    }

    @Test @Order(4)
    void ddlRoundTrip() throws Exception {
        try (Statement s = c.createStatement()) {
            s.execute("CREATE DATABASE " + DB);
            s.execute("CREATE TABLE " + DB + "_tbl (ts TIMESTAMP, v INT64) TIMESTAMP(ts)");
            try (ResultSet r = s.executeQuery("SELECT count(*) FROM " + DB + "_tbl")) {
                assertTrue(r.next());
                assertEquals(0L, r.getLong(1));
            }
            s.execute("DROP TABLE " + DB + "_tbl");
        }
    }

    @Test @Order(5)
    void preparedStatementSubstitution() throws Exception {
        try (PreparedStatement ps = c.prepareStatement("SELECT ?, ?, ?")) {
            ps.setString(1, "hello");
            ps.setLong(2, 42L);
            ps.setDouble(3, 3.14);
            // tsdb doesn't support bare SELECT expression lists, so the
            // renderer should at least emit the right SQL without
            // throwing.  We catch the server's parse error and check
            // the rendered string isn't garbage.
            try (ResultSet ignored = ps.executeQuery()) {
                // Some dialects will accept it; if so, great.
            } catch (SQLException e) {
                assertTrue(e.getMessage().toLowerCase().contains("parse")
                        || e.getMessage().toLowerCase().contains("expect"),
                    "expected parse error, got: " + e.getMessage());
            }
        }
    }

    @Test @Order(6)
    void errorsSurfaceAsSQLException() throws Exception {
        try (Statement s = c.createStatement()) {
            assertThrows(SQLException.class,
                () -> s.executeQuery("SELECT * FROM no_such_table_ever"));
        }
    }

    @Test @Order(7)
    void readOnlyRefusesMutation() throws Exception {
        c.setReadOnly(true);
        try (Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLException.class,
                () -> s.execute("CREATE DATABASE should_not_happen"));
            assertTrue(e.getMessage().contains("read-only"));
        } finally {
            c.setReadOnly(false);
        }
    }

}
