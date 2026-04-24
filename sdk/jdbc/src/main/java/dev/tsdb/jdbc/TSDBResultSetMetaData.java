package dev.tsdb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public final class TSDBResultSetMetaData implements ResultSetMetaData {

    private final String[] cols;
    private final String[] types;

    TSDBResultSetMetaData(String[] cols, String[] types) {
        this.cols = cols;
        this.types = types;
    }

    @Override public int     getColumnCount()             { return cols.length; }
    @Override public String  getColumnName(int i)         { return cols[i - 1]; }
    @Override public String  getColumnLabel(int i)        { return cols[i - 1]; }
    @Override public int     getColumnType(int i)         { return TSDBResultSet.sqlTypeOf(types[i - 1]); }
    @Override public String  getColumnTypeName(int i)     { return types[i - 1]; }
    @Override public boolean isAutoIncrement(int i)       { return false; }
    @Override public boolean isCaseSensitive(int i)       { return true; }
    @Override public boolean isSearchable(int i)          { return true; }
    @Override public boolean isCurrency(int i)            { return false; }
    @Override public int     isNullable(int i)            { return columnNullableUnknown; }
    @Override public boolean isSigned(int i) {
        String t = types[i - 1];
        return "INT64".equalsIgnoreCase(t) || "FLOAT64".equalsIgnoreCase(t);
    }
    @Override public int     getColumnDisplaySize(int i)  { return 32; }
    @Override public String  getSchemaName(int i)         { return ""; }
    @Override public String  getTableName(int i)          { return ""; }
    @Override public String  getCatalogName(int i)        { return ""; }
    @Override public int     getPrecision(int i)          { return 0; }
    @Override public int     getScale(int i)              { return 0; }
    @Override public boolean isReadOnly(int i)            { return true; }
    @Override public boolean isWritable(int i)            { return false; }
    @Override public boolean isDefinitelyWritable(int i)  { return false; }
    @Override public String  getColumnClassName(int i) {
        switch (types[i - 1] == null ? "" : types[i - 1].toUpperCase()) {
            case "INT64":     return "java.lang.Long";
            case "FLOAT64":   return "java.lang.Double";
            case "TIMESTAMP": return "java.sql.Timestamp";
            case "SYMBOL":    return "java.lang.String";
            case "BOOL":      return "java.lang.Boolean";
            default:          return "java.lang.Object";
        }
    }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName());
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
}
