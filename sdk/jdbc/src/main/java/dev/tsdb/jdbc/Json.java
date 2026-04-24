package dev.tsdb.jdbc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON parser — only what the JDBC driver needs to read the
 * {@code /sql} response envelope.  Supports objects, arrays, strings,
 * doubles / longs, booleans and null.  No external dependency.
 *
 * <p>The parser is lenient in the ways we need it to be: large integers
 * outside the {@code long} range fall back to {@code double} so the
 * caller decides how to surface them.  We never emit BigDecimal —
 * the {@code /sql} response types are always one of
 * SYMBOL / INT64 / FLOAT64 / TIMESTAMP / BOOL.
 */
final class Json {
    private final String s;
    private int i;

    private Json(String src) { this.s = src; this.i = 0; }

    static Object parse(String src) {
        Json p = new Json(src);
        p.skipWs();
        Object v = p.readValue();
        p.skipWs();
        if (p.i != p.s.length())
            throw new IllegalArgumentException("trailing garbage at " + p.i);
        return v;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asObject(Object v) {
        if (!(v instanceof Map))
            throw new IllegalStateException("expected object, got " + (v == null ? "null" : v.getClass().getSimpleName()));
        return (Map<String, Object>) v;
    }

    @SuppressWarnings("unchecked")
    static List<Object> asArray(Object v) {
        if (!(v instanceof List))
            throw new IllegalStateException("expected array, got " + (v == null ? "null" : v.getClass().getSimpleName()));
        return (List<Object>) v;
    }

    static String asString(Object v) { return v == null ? null : String.valueOf(v); }

    private Object readValue() {
        char c = peek();
        if (c == '{')      return readObject();
        if (c == '[')      return readArray();
        if (c == '"')      return readString();
        if (c == 't' || c == 'f') return readBool();
        if (c == 'n')      return readNull();
        return readNumber();
    }

    private Map<String, Object> readObject() {
        expect('{');
        Map<String, Object> m = new LinkedHashMap<>();
        skipWs();
        if (peek() == '}') { i++; return m; }
        while (true) {
            skipWs();
            String key = readString();
            skipWs();
            expect(':');
            skipWs();
            m.put(key, readValue());
            skipWs();
            char c = peek();
            if (c == ',') { i++; continue; }
            if (c == '}') { i++; return m; }
            throw new IllegalArgumentException("expected , or } at " + i);
        }
    }

    private List<Object> readArray() {
        expect('[');
        List<Object> out = new ArrayList<>();
        skipWs();
        if (peek() == ']') { i++; return out; }
        while (true) {
            skipWs();
            out.add(readValue());
            skipWs();
            char c = peek();
            if (c == ',') { i++; continue; }
            if (c == ']') { i++; return out; }
            throw new IllegalArgumentException("expected , or ] at " + i);
        }
    }

    private String readString() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (i < s.length()) {
            char c = s.charAt(i++);
            if (c == '"')  return sb.toString();
            if (c == '\\') {
                if (i >= s.length()) throw new IllegalArgumentException("trailing \\");
                char e = s.charAt(i++);
                switch (e) {
                    case '"':  sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/':  sb.append('/'); break;
                    case 'n':  sb.append('\n'); break;
                    case 'r':  sb.append('\r'); break;
                    case 't':  sb.append('\t'); break;
                    case 'b':  sb.append('\b'); break;
                    case 'f':  sb.append('\f'); break;
                    case 'u':
                        if (i + 4 > s.length()) throw new IllegalArgumentException("bad \\u");
                        int cp = Integer.parseInt(s.substring(i, i + 4), 16);
                        sb.append((char) cp);
                        i += 4; break;
                    default: throw new IllegalArgumentException("bad escape \\" + e);
                }
            } else {
                sb.append(c);
            }
        }
        throw new IllegalArgumentException("unterminated string");
    }

    private Object readNumber() {
        int start = i;
        if (peek() == '-') i++;
        while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
        boolean fp = false;
        if (i < s.length() && s.charAt(i) == '.') { fp = true; i++; while (i < s.length() && Character.isDigit(s.charAt(i))) i++; }
        if (i < s.length() && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
            fp = true; i++;
            if (i < s.length() && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
            while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
        }
        String lit = s.substring(start, i);
        if (!fp) {
            try { return Long.parseLong(lit); }
            catch (NumberFormatException ignore) { /* overflow → fall through */ }
        }
        return Double.parseDouble(lit);
    }

    private Boolean readBool() {
        if (s.startsWith("true",  i)) { i += 4; return Boolean.TRUE; }
        if (s.startsWith("false", i)) { i += 5; return Boolean.FALSE; }
        throw new IllegalArgumentException("bad bool at " + i);
    }

    private Object readNull() {
        if (s.startsWith("null", i)) { i += 4; return null; }
        throw new IllegalArgumentException("bad null at " + i);
    }

    private char peek() {
        if (i >= s.length()) throw new IllegalArgumentException("unexpected EOF");
        return s.charAt(i);
    }
    private void expect(char c) {
        if (peek() != c) throw new IllegalArgumentException("expected '" + c + "' at " + i);
        i++;
    }
    private void skipWs() {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') i++;
            else break;
        }
    }

    /* ---- tiny writer for request bodies ---- */
    static String encodeString(String v) {
        StringBuilder sb = new StringBuilder(v.length() + 8);
        sb.append('"');
        for (int k = 0; k < v.length(); k++) {
            char c = v.charAt(k);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
