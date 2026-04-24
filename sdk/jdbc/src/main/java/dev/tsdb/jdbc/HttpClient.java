package dev.tsdb.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HTTP transport used by {@link TSDBConnection}.  Handles three
 * responsibilities on top of {@link HttpURLConnection}:
 *
 * <ul>
 *   <li>Session cookie management — issues POST /login on open, stores
 *       the {@code tsdb_auth} cookie, and re-attaches it on every
 *       subsequent request.</li>
 *   <li>Connection keep-alive — HttpURLConnection pools by URL, so
 *       single-host traffic reuses the same socket.</li>
 *   <li>Error propagation — converts 401 to "login required" and
 *       non-2xx bodies to readable exception messages.</li>
 * </ul>
 *
 * The driver never needs TLS cert pinning today; {@code https://} URLs
 * just work through the JVM's default trust store.
 */
final class HttpClient {
    private final String baseUrl;
    private final int timeoutMs;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<String> cookies = new ArrayList<>();

    HttpClient(String baseUrl, int timeoutMs) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.timeoutMs = timeoutMs;
    }

    void login(String user, String password) throws IOException {
        String body = "user=" + enc(user) + "&pass=" + enc(password);
        Response r = send("POST", "/login",
                "application/x-www-form-urlencoded",
                body.getBytes(StandardCharsets.UTF_8),
                /*followRedirect*/ false);
        // The server answers 302 on success with a Set-Cookie header;
        // captureCookies picked it up below.  401 means wrong creds.
        if (r.status == 401)
            throw new IOException("login failed: invalid credentials");
        if (r.status != 200 && r.status != 302 && r.status != 303)
            throw new IOException("login returned HTTP " + r.status);
        // Double-check that at least one cookie was captured.
        if (cookiesEmpty())
            throw new IOException("login succeeded (" + r.status + ") but no session cookie returned");
    }

    /** Run one SQL/QTL statement and return the raw JSON body. */
    String postSql(String sqlBody) throws IOException {
        byte[] body = sqlBody.getBytes(StandardCharsets.UTF_8);
        Response r = send("POST", "/sql", "application/json", body, true);
        if (r.status == 401)
            throw new IOException("session expired; re-login required");
        if (r.status >= 500)
            throw new IOException("tsdb /sql returned HTTP " + r.status + ": " + r.bodyText());
        return r.bodyText();
    }

    /** Close the session; cookie is invalidated server-side. */
    void logout() {
        try {
            send("GET", "/logout", null, null, false);
        } catch (IOException ignored) {
            // Best-effort; nothing sensible to do on logout failure.
        }
        lock.lock();
        try { cookies.clear(); }
        finally { lock.unlock(); }
    }

    private boolean cookiesEmpty() {
        lock.lock();
        try { return cookies.isEmpty(); }
        finally { lock.unlock(); }
    }

    private Response send(String method, String path, String contentType,
                           byte[] body, boolean followRedirect) throws IOException {
        URL u = new URL(baseUrl + path);
        HttpURLConnection c = (HttpURLConnection) u.openConnection();
        c.setRequestMethod(method);
        c.setConnectTimeout(timeoutMs);
        c.setReadTimeout(timeoutMs);
        c.setInstanceFollowRedirects(followRedirect);

        String cookieHdr = joinCookies();
        if (!cookieHdr.isEmpty()) c.setRequestProperty("Cookie", cookieHdr);
        if (contentType != null) c.setRequestProperty("Content-Type", contentType);
        c.setRequestProperty("Accept", "application/json, text/plain;q=0.9");

        if (body != null) {
            c.setDoOutput(true);
            c.setFixedLengthStreamingMode(body.length);
            try (OutputStream os = c.getOutputStream()) {
                os.write(body);
            }
        }
        int status = c.getResponseCode();
        captureCookies(c);

        byte[] rb;
        try (InputStream is = status >= 400 ? c.getErrorStream() : c.getInputStream()) {
            rb = is == null ? new byte[0] : readAll(is);
        }
        Response r = new Response();
        r.status = status;
        r.body = rb;
        return r;
    }

    private String joinCookies() {
        lock.lock();
        try {
            if (cookies.isEmpty()) return "";
            StringBuilder sb = new StringBuilder();
            for (int k = 0; k < cookies.size(); k++) {
                if (k > 0) sb.append("; ");
                sb.append(cookies.get(k));
            }
            return sb.toString();
        } finally { lock.unlock(); }
    }

    private void captureCookies(HttpURLConnection c) {
        List<String> raw = c.getHeaderFields().get("Set-Cookie");
        if (raw == null) return;
        lock.lock();
        try {
            for (String s : raw) {
                // Take only "name=value" from "name=value; Path=/; HttpOnly".
                int semi = s.indexOf(';');
                String nv = semi < 0 ? s : s.substring(0, semi);
                // Replace existing cookie of the same name instead of piling them up.
                int eq = nv.indexOf('=');
                if (eq < 0) continue;
                String name = nv.substring(0, eq);
                cookies.removeIf(old -> old.startsWith(name + "="));
                cookies.add(nv);
            }
        } finally { lock.unlock(); }
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[16 * 1024];
        int n;
        while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
        return out.toByteArray();
    }

    private static String enc(String v) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < v.length(); k++) {
            char c = v.charAt(k);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~') {
                sb.append(c);
            } else {
                // Percent-encode bytes individually (good enough for
                // ASCII usernames / passwords; the server accepts UTF-8).
                byte[] bs = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                for (byte b : bs) sb.append(String.format("%%%02X", b & 0xFF));
            }
        }
        return sb.toString();
    }

    static final class Response {
        int status;
        byte[] body;
        String bodyText() { return new String(body, StandardCharsets.UTF_8); }
    }
}
