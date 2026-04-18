package com.qengine.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

/**
 * Column-level metadata for a QEngine result set.
 * Types are derived from the QUERY_RESULT_HDR frame.
 */
public class QEngineResultSetMetaData implements ResultSetMetaData {

    private final String[] names;
    private final byte[]   types; // QEngine wire type codes

    QEngineResultSetMetaData(String[] names, byte[] types) {
        this.names = names;
        this.types = types;
    }

    private void checkIdx(int column) throws SQLException {
        if (column < 1 || column > names.length) {
            throw new QEngineWireException("column index out of range: " + column);
        }
    }

    @Override public int getColumnCount() { return names.length; }
    @Override public boolean isAutoIncrement(int c) throws SQLException { checkIdx(c); return false; }
    @Override public boolean isCaseSensitive(int c) throws SQLException { checkIdx(c); return true; }
    @Override public boolean isSearchable(int c)    throws SQLException { checkIdx(c); return true; }
    @Override public boolean isCurrency(int c)      throws SQLException { checkIdx(c); return false; }
    @Override public int     isNullable(int c)      throws SQLException { checkIdx(c); return columnNullableUnknown; }
    @Override public boolean isSigned(int c) throws SQLException {
        checkIdx(c);
        byte t = types[c - 1];
        return t == QEngineConnection.TYPE_INT64 || t == QEngineConnection.TYPE_FLOAT64;
    }
    @Override public int getColumnDisplaySize(int c) throws SQLException { checkIdx(c); return 32; }
    @Override public String getColumnLabel(int c)    throws SQLException { checkIdx(c); return names[c - 1]; }
    @Override public String getColumnName(int c)     throws SQLException { checkIdx(c); return names[c - 1]; }
    @Override public String getSchemaName(int c)     throws SQLException { checkIdx(c); return ""; }
    @Override public int    getPrecision(int c)      throws SQLException { checkIdx(c); return 0; }
    @Override public int    getScale(int c)          throws SQLException { checkIdx(c); return 0; }
    @Override public String getTableName(int c)      throws SQLException { checkIdx(c); return ""; }
    @Override public String getCatalogName(int c)    throws SQLException { checkIdx(c); return ""; }

    @Override public int getColumnType(int c) throws SQLException {
        checkIdx(c);
        switch (types[c - 1]) {
            case QEngineConnection.TYPE_TIMESTAMP: return Types.TIMESTAMP;
            case QEngineConnection.TYPE_INT64:     return Types.BIGINT;
            case QEngineConnection.TYPE_FLOAT64:   return Types.DOUBLE;
            case QEngineConnection.TYPE_SYMBOL:    return Types.VARCHAR;
            default: return Types.OTHER;
        }
    }

    @Override public String getColumnTypeName(int c) throws SQLException {
        checkIdx(c);
        switch (types[c - 1]) {
            case QEngineConnection.TYPE_TIMESTAMP: return "TIMESTAMP";
            case QEngineConnection.TYPE_INT64:     return "INT64";
            case QEngineConnection.TYPE_FLOAT64:   return "FLOAT64";
            case QEngineConnection.TYPE_SYMBOL:    return "SYMBOL";
            default: return "UNKNOWN";
        }
    }

    @Override public boolean isReadOnly(int c)   throws SQLException { checkIdx(c); return true; }
    @Override public boolean isWritable(int c)   throws SQLException { checkIdx(c); return false; }
    @Override public boolean isDefinitelyWritable(int c) throws SQLException { checkIdx(c); return false; }

    @Override public String getColumnClassName(int c) throws SQLException {
        checkIdx(c);
        switch (types[c - 1]) {
            case QEngineConnection.TYPE_TIMESTAMP: return "java.sql.Timestamp";
            case QEngineConnection.TYPE_INT64:     return "java.lang.Long";
            case QEngineConnection.TYPE_FLOAT64:   return "java.lang.Double";
            case QEngineConnection.TYPE_SYMBOL:    return "java.lang.String";
            default: return "java.lang.Object";
        }
    }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLFeatureNotSupportedException("unwrap");
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isAssignableFrom(getClass()); }

    byte   getRawType(int c)   { return types[c - 1]; }
    String getName(int c)      { return names[c - 1]; }
}
