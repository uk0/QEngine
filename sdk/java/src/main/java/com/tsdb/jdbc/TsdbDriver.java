package com.tsdb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal JDBC driver for QEngine (tsdb).
 *
 * <p>URL format:
 * <pre>
 *   jdbc:tsdb://host:port[/?user=NAME&amp;password=PW]
 * </pre>
 *
 * <p>On class load, registers itself with {@link DriverManager}.  Only the
 * read path (createStatement + executeQuery) and basic write + DDL through
 * executeUpdate are implemented.  Unsupported JDBC calls throw
 * {@link SQLFeatureNotSupportedException} — no silent NO-OPs.
 */
public class TsdbDriver implements Driver {

    public static final int MAJOR = 0;
    public static final int MINOR = 1;

    static {
        try {
            DriverManager.registerDriver(new TsdbDriver());
        } catch (SQLException e) {
            throw new RuntimeException("tsdb JDBC register failed", e);
        }
    }

    private static final Pattern URL_RE =
            Pattern.compile("^jdbc:tsdb://([^:/]+):(\\d+)(?:/)?(?:\\?(.+))?$");

    @Override public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:tsdb://");
    }

    @Override public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) return null;
        Matcher m = URL_RE.matcher(url);
        if (!m.matches()) throw new SQLException("bad URL: " + url);
        String host = m.group(1);
        int    port = Integer.parseInt(m.group(2));
        String query = m.group(3);

        Properties eff = props == null ? new Properties() : (Properties) props.clone();
        if (query != null) {
            for (String kv : query.split("&")) {
                int eq = kv.indexOf('=');
                if (eq > 0) eff.setProperty(kv.substring(0, eq), kv.substring(eq + 1));
            }
        }
        String user = eff.getProperty("user");
        String pass = eff.getProperty("password");
        return new TsdbConnection(host, port, user, pass);
    }

    @Override public int getMajorVersion() { return MAJOR; }
    @Override public int getMinorVersion() { return MINOR; }
    @Override public boolean jdbcCompliant() { return false; }

    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[] {
                new DriverPropertyInfo("user", info == null ? null : info.getProperty("user")),
                new DriverPropertyInfo("password", info == null ? null : info.getProperty("password"))
        };
    }

    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
