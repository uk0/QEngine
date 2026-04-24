package dev.tsdb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver entry point.  Registered via
 * {@code META-INF/services/java.sql.Driver} so
 * {@link DriverManager} auto-discovers it at first use.
 *
 * <p>URL grammar:
 * <pre>
 *   jdbc:tsdb://host:port[/db][?user=root&amp;pass=secret&amp;timeout_ms=10000&amp;tls=true]
 * </pre>
 * The {@code db} path segment is informational only (the server is
 * agnostic per-connection), but client code reads it via
 * {@link TSDBConnection#getCatalog()} / {@code setCatalog}.
 *
 * <p>Accepted query-string and {@link Properties} keys:
 * <ul>
 *   <li>{@code user} — login name (default {@code root})</li>
 *   <li>{@code pass} — password</li>
 *   <li>{@code timeout_ms} — HTTP connect + read timeout (default 30 000)</li>
 *   <li>{@code tls} — set to {@code true} to force https:// (default derived from port)</li>
 *   <li>{@code fetch_size} — default fetch size for queries (default 0 = all)</li>
 * </ul>
 */
public final class TSDBDriver implements Driver {

    private static final Logger LOG = Logger.getLogger("dev.tsdb.jdbc");
    private static final String PREFIX = "jdbc:tsdb://";

    public static final int MAJOR = 0;
    public static final int MINOR = 1;

    static {
        try { DriverManager.registerDriver(new TSDBDriver()); }
        catch (SQLException e) { throw new ExceptionInInitializerError(e); }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null; // JDBC contract: non-match returns null
        Map<String, String> q = parse(url);
        if (info != null) {
            for (String k : info.stringPropertyNames()) q.putIfAbsent(k, info.getProperty(k));
        }
        return new TSDBConnection(url, q);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo user = new DriverPropertyInfo("user", info == null ? "" : info.getProperty("user", ""));
        user.required = true;
        user.description = "tsdb login name (default: root)";
        DriverPropertyInfo pass = new DriverPropertyInfo("pass", info == null ? "" : info.getProperty("pass", ""));
        pass.required = true;
        pass.description = "tsdb password";
        DriverPropertyInfo timeout = new DriverPropertyInfo("timeout_ms", "30000");
        timeout.required = false;
        timeout.description = "HTTP connect + read timeout";
        return new DriverPropertyInfo[]{ user, pass, timeout };
    }

    @Override public int  getMajorVersion()   { return MAJOR; }
    @Override public int  getMinorVersion()   { return MINOR; }
    @Override public boolean jdbcCompliant()  { return false; }
    @Override public Logger getParentLogger() { return LOG; }

    /** Parse "jdbc:tsdb://host:port[/db][?k=v&...]" into a k→v map, with
     *  {@code host} and {@code port} pulled out as synthetic keys. */
    static Map<String, String> parse(String url) throws SQLException {
        if (!url.startsWith(PREFIX))
            throw new SQLException("not a tsdb JDBC URL: " + url);
        String rest = url.substring(PREFIX.length());
        // Split off the optional query string.
        String qs = "";
        int qMark = rest.indexOf('?');
        if (qMark >= 0) { qs = rest.substring(qMark + 1); rest = rest.substring(0, qMark); }
        // "host:port[/db]"
        String db = "";
        int slash = rest.indexOf('/');
        if (slash >= 0) { db = rest.substring(slash + 1); rest = rest.substring(0, slash); }
        String host; int port;
        int colon = rest.lastIndexOf(':');
        if (colon < 0) {
            host = rest; port = 28094;
        } else {
            host = rest.substring(0, colon);
            try { port = Integer.parseInt(rest.substring(colon + 1)); }
            catch (NumberFormatException e) { throw new SQLException("bad port in URL: " + url); }
        }
        Map<String, String> m = new HashMap<>();
        m.put("host", host);
        m.put("port", String.valueOf(port));
        if (!db.isEmpty()) m.put("db", db);
        for (String pair : qs.split("&")) {
            if (pair.isEmpty()) continue;
            int eq = pair.indexOf('=');
            if (eq < 0) { m.put(pair, ""); continue; }
            m.put(pair.substring(0, eq), urlDecode(pair.substring(eq + 1)));
        }
        return m;
    }

    private static String urlDecode(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length()) {
                int b = Integer.parseInt(s.substring(i + 1, i + 3), 16);
                sb.append((char) b);
                i += 2;
            } else if (c == '+') {
                sb.append(' ');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
