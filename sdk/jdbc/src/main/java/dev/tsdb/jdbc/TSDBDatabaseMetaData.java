package dev.tsdb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * Minimal {@link DatabaseMetaData} — enough for ORMs like DBeaver /
 * JetBrains DB tools to recognise the connection without crashing.
 * Catalog / schema / table discovery isn't implemented yet (the ORM
 * either falls back to its own SQL or politely shows "no tables").
 */
public final class TSDBDatabaseMetaData implements DatabaseMetaData {

    private final TSDBConnection conn;
    private final String url;

    TSDBDatabaseMetaData(TSDBConnection conn, String url) {
        this.conn = conn; this.url = url;
    }

    @Override public Connection getConnection()            { return conn; }
    @Override public String     getURL()                   { return url; }
    @Override public String     getUserName()              { return "root"; }
    @Override public String     getDatabaseProductName()   { return "tsdb"; }
    @Override public String     getDatabaseProductVersion(){ return "0.1"; }
    @Override public String     getDriverName()            { return "tsdb-jdbc"; }
    @Override public String     getDriverVersion()         { return TSDBDriver.MAJOR + "." + TSDBDriver.MINOR; }
    @Override public int        getDriverMajorVersion()    { return TSDBDriver.MAJOR; }
    @Override public int        getDriverMinorVersion()    { return TSDBDriver.MINOR; }
    @Override public int        getDatabaseMajorVersion()  { return 0; }
    @Override public int        getDatabaseMinorVersion()  { return 1; }
    @Override public int        getJDBCMajorVersion()      { return 4; }
    @Override public int        getJDBCMinorVersion()      { return 2; }
    @Override public int        getDefaultTransactionIsolation() { return Connection.TRANSACTION_NONE; }
    @Override public boolean    supportsTransactions()       { return false; }
    @Override public boolean    supportsTransactionIsolationLevel(int level) { return level == Connection.TRANSACTION_NONE; }
    @Override public boolean    supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean    supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean    supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean    supportsANSI92EntryLevelSQL(){ return false; }
    @Override public boolean    supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean    supportsANSI92FullSQL()      { return false; }
    @Override public boolean    isReadOnly()               { return conn.isReadOnly(); }
    @Override public String     getIdentifierQuoteString() { return "\""; }
    @Override public String     getSQLKeywords() {
        return "LIST,VTABLE,PTABLE,STABLE,TAGS,TIMESTAMP,SYMBOL,BUCKET,PARTITION,BLOCK_POINTS";
    }
    @Override public String     getNumericFunctions()  { return "avg,sum,min,max,stddev,count"; }
    @Override public String     getStringFunctions()   { return ""; }
    @Override public String     getSystemFunctions()   { return ""; }
    @Override public String     getTimeDateFunctions() { return "now,time_bucket,to_timestamp"; }
    @Override public String     getSearchStringEscape(){ return "\\"; }
    @Override public String     getExtraNameCharacters(){ return ""; }

    /* Many *As methods — default to "no clue / false" which is fine for
     * 95% of introspection clients.  We return empty ResultSets for the
     * getters that require columnar results so DBeaver / JetBrains don't
     * NPE on null. */

    @Override public boolean allProceduresAreCallable()    { return false; }
    @Override public boolean allTablesAreSelectable()      { return true; }
    @Override public boolean nullsAreSortedHigh()          { return false; }
    @Override public boolean nullsAreSortedLow()           { return true; }
    @Override public boolean nullsAreSortedAtStart()       { return false; }
    @Override public boolean nullsAreSortedAtEnd()         { return false; }
    @Override public boolean usesLocalFiles()              { return false; }
    @Override public boolean usesLocalFilePerTable()       { return true; }
    @Override public boolean supportsMixedCaseIdentifiers(){ return true; }
    @Override public boolean storesUpperCaseIdentifiers()  { return false; }
    @Override public boolean storesLowerCaseIdentifiers()  { return false; }
    @Override public boolean storesMixedCaseIdentifiers()  { return true; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers()   { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers()   { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers()   { return true; }

    @Override public boolean supportsAlterTableWithAddColumn() { return true; }
    @Override public boolean supportsAlterTableWithDropColumn(){ return false; }
    @Override public boolean supportsColumnAliasing()          { return true; }
    @Override public boolean nullPlusNonNullIsNull()           { return true; }
    @Override public boolean supportsConvert()                 { return false; }
    @Override public boolean supportsConvert(int from, int to) { return false; }
    @Override public boolean supportsTableCorrelationNames()   { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return true; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated()     { return false; }
    @Override public boolean supportsGroupBy()              { return true; }
    @Override public boolean supportsGroupByUnrelated()     { return false; }
    @Override public boolean supportsGroupByBeyondSelect()  { return false; }
    @Override public boolean supportsLikeEscapeClause()     { return false; }
    @Override public boolean supportsMultipleResultSets()   { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsNonNullableColumns()   { return true; }
    @Override public boolean supportsMinimumSQLGrammar()    { return false; }
    @Override public boolean supportsCoreSQLGrammar()       { return false; }
    @Override public boolean supportsExtendedSQLGrammar()   { return false; }
    @Override public boolean supportsOuterJoins()           { return false; }
    @Override public boolean supportsFullOuterJoins()       { return false; }
    @Override public boolean supportsLimitedOuterJoins()    { return false; }
    @Override public String  getSchemaTerm()                { return "database"; }
    @Override public String  getProcedureTerm()             { return "procedure"; }
    @Override public String  getCatalogTerm()               { return "catalog"; }
    @Override public boolean isCatalogAtStart()             { return true; }
    @Override public String  getCatalogSeparator()          { return "."; }
    @Override public boolean supportsSchemasInDataManipulation() { return true; }
    @Override public boolean supportsSchemasInProcedureCalls()   { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return true; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return true; }
    @Override public boolean supportsCatalogsInDataManipulation()    { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls()      { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions()    { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions()    { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions(){ return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsSelectForUpdate()  { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsSubqueriesInComparisons() { return false; }
    @Override public boolean supportsSubqueriesInExists()      { return false; }
    @Override public boolean supportsSubqueriesInIns()         { return false; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return false; }
    @Override public boolean supportsCorrelatedSubqueries()    { return false; }
    @Override public boolean supportsUnion()      { return false; }
    @Override public boolean supportsUnionAll()   { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit()      { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback()    { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit()   { return true; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return true; }
    @Override public int  getMaxBinaryLiteralLength()   { return 0; }
    @Override public int  getMaxCharLiteralLength()     { return 0; }
    @Override public int  getMaxColumnNameLength()      { return 64; }
    @Override public int  getMaxColumnsInGroupBy()      { return 0; }
    @Override public int  getMaxColumnsInIndex()        { return 0; }
    @Override public int  getMaxColumnsInOrderBy()      { return 0; }
    @Override public int  getMaxColumnsInSelect()       { return 0; }
    @Override public int  getMaxColumnsInTable()        { return 0; }
    @Override public int  getMaxConnections()           { return 0; }
    @Override public int  getMaxCursorNameLength()      { return 0; }
    @Override public int  getMaxIndexLength()           { return 0; }
    @Override public int  getMaxSchemaNameLength()      { return 64; }
    @Override public int  getMaxProcedureNameLength()   { return 0; }
    @Override public int  getMaxCatalogNameLength()     { return 0; }
    @Override public int  getMaxRowSize()               { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    @Override public int  getMaxStatementLength()       { return 0; }
    @Override public int  getMaxStatements()            { return 0; }
    @Override public int  getMaxTableNameLength()       { return 64; }
    @Override public int  getMaxTablesInSelect()        { return 8; }
    @Override public int  getMaxUserNameLength()        { return 64; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return true; }
    @Override public boolean dataDefinitionIgnoredInTransactions()   { return false; }
    @Override public ResultSet getProcedures(String a, String b, String c) throws SQLException { return empty(); }
    @Override public ResultSet getProcedureColumns(String a, String b, String c, String d) throws SQLException { return empty(); }
    @Override public ResultSet getTables(String catalog, String schemaPattern, String tablePattern, String[] types) throws SQLException { return empty(); }
    @Override public ResultSet getSchemas() throws SQLException { return empty(); }
    @Override public ResultSet getSchemas(String c, String s) throws SQLException { return empty(); }
    @Override public ResultSet getCatalogs() throws SQLException { return empty(); }
    @Override public ResultSet getTableTypes() throws SQLException { return empty(); }
    @Override public ResultSet getColumns(String c, String s, String t, String col) throws SQLException { return empty(); }
    @Override public ResultSet getColumnPrivileges(String c, String s, String t, String col) throws SQLException { return empty(); }
    @Override public ResultSet getTablePrivileges(String c, String s, String t) throws SQLException { return empty(); }
    @Override public ResultSet getBestRowIdentifier(String c, String s, String t, int scope, boolean nullable) throws SQLException { return empty(); }
    @Override public ResultSet getVersionColumns(String c, String s, String t) throws SQLException { return empty(); }
    @Override public ResultSet getPrimaryKeys(String c, String s, String t) throws SQLException { return empty(); }
    @Override public ResultSet getImportedKeys(String c, String s, String t) throws SQLException { return empty(); }
    @Override public ResultSet getExportedKeys(String c, String s, String t) throws SQLException { return empty(); }
    @Override public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft) throws SQLException { return empty(); }
    @Override public ResultSet getTypeInfo() throws SQLException { return empty(); }
    @Override public ResultSet getIndexInfo(String c, String s, String t, boolean unique, boolean approx) throws SQLException { return empty(); }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int t, int c) { return t == ResultSet.TYPE_FORWARD_ONLY && c == ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean ownUpdatesAreVisible(int type)        { return false; }
    @Override public boolean ownDeletesAreVisible(int type)        { return false; }
    @Override public boolean ownInsertsAreVisible(int type)        { return false; }
    @Override public boolean othersUpdatesAreVisible(int type)     { return false; }
    @Override public boolean othersDeletesAreVisible(int type)     { return false; }
    @Override public boolean othersInsertsAreVisible(int type)     { return false; }
    @Override public boolean updatesAreDetected(int type)          { return false; }
    @Override public boolean deletesAreDetected(int type)          { return false; }
    @Override public boolean insertsAreDetected(int type)          { return false; }
    @Override public boolean supportsBatchUpdates()                { return false; }
    @Override public ResultSet getUDTs(String c, String s, String t, int[] types) throws SQLException { return empty(); }
    @Override public boolean supportsSavepoints()      { return false; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsGetGeneratedKeys()    { return false; }
    @Override public ResultSet getSuperTypes(String c, String s, String t) throws SQLException { return empty(); }
    @Override public ResultSet getSuperTables(String c, String s, String t) throws SQLException { return empty(); }
    @Override public ResultSet getAttributes(String c, String s, String t, String a) throws SQLException { return empty(); }
    @Override public boolean supportsResultSetHoldability(int h)      { return h == ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public int     getResultSetHoldability()                { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public int     getSQLStateType()                        { return sqlStateSQL; }
    @Override public boolean locatorsUpdateCopy()                     { return false; }
    @Override public boolean supportsStatementPooling()               { return false; }
    @Override public RowIdLifetime getRowIdLifetime()                 { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets()   { return false; }
    @Override public ResultSet getClientInfoProperties() throws SQLException { return empty(); }
    @Override public ResultSet getFunctions(String c, String s, String f) throws SQLException { return empty(); }
    @Override public ResultSet getFunctionColumns(String c, String s, String f, String col) throws SQLException { return empty(); }
    @Override public ResultSet getPseudoColumns(String c, String s, String t, String col) throws SQLException { return empty(); }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("not a wrapper for " + iface.getName());
    }
    @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private static ResultSet empty() throws SQLException {
        return TSDBResultSet.fromJson(null, "{\"cols\":[],\"types\":[],\"rows\":[],\"nrows\":0,\"truncated\":false,\"ms\":0}");
    }
}
