package com.poc.CanonicalIngestionEngine.sql;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 🎯 CORE CONCEPT: Dynamic SQL Generation
 *
 * This class generates INSERT SQL statements dynamically based on:
 * 1. Table name from YAML
 * 2. Mapped columns from YAML
 * 3. Actual database columns (fetched from Oracle metadata)
 *
 * Why? To prevent "missing parameter" errors and ensure we only
 * insert columns that actually exist in the database.
 */
@Component
public class DynamicSqlBuilder {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // Cache database columns to avoid repeated queries
    private final Map<String, Set<String>> dbColumnCache = new HashMap<>();

    /**
     * 🔥 Main method: Build INSERT SQL dynamically
     *
     * @param tableName - Database table name (e.g., "SEND_TRANSACTIONS")
     * @param mappedColumns - Columns from YAML mapping (e.g., ["TRAN_ID", "TRAN_TYPE"])
     * @param autoGenerateId - Whether to add "ID" column automatically
     * @return Complete INSERT SQL statement
     */
    public String buildInsertSql(String tableName,
                                 Set<String> mappedColumns,
                                 boolean autoGenerateId) {

        System.out.println("   🔧 Building SQL for table: " + tableName);
        System.out.println("      📝 Mapped columns from YAML: " + mappedColumns);

        // Step 1: Get actual columns from database
        Set<String> dbColumns = getDbColumns(tableName);
        System.out.println("      💾 DB columns: " + dbColumns);

        // Step 2: Start with mapped columns
        Set<String> finalColumns = new HashSet<>(mappedColumns);

        // Step 3: Add ID column if needed (for recipient/address tables)
        if (autoGenerateId) {
            finalColumns.add("ID");
            System.out.println("      ➕ Added ID column (autoGenerateId=true)");
        }

        // Step 4: Filter - keep only columns that exist in DB
        finalColumns.retainAll(dbColumns);
        System.out.println("      ✅ Final columns to insert: " + finalColumns);

        // Step 5: Build SQL
        String sql = buildSql(tableName, finalColumns);
        System.out.println("      📜 Generated SQL: " + sql);

        return sql;
    }

    /**
     * 🔍 Fetch actual column names from Oracle database
     */
    private Set<String> getDbColumns(String tableName) {

        // Check cache first
        if (dbColumnCache.containsKey(tableName)) {
            return dbColumnCache.get(tableName);
        }

        // Query Oracle metadata
        String sql = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?";

        List<String> columns = jdbcTemplate.queryForList(
                sql,
                String.class,
                tableName.toUpperCase()
        );

        Set<String> columnSet = new HashSet<>(columns);

        // Cache it
        dbColumnCache.put(tableName, columnSet);

        return columnSet;
    }

    /**
     * 🛠️ Build the actual INSERT SQL string
     *
     * Example output:
     * INSERT INTO SEND_TRANSACTIONS (TRAN_ID, TRAN_TYPE, ACCT_NUM)
     * VALUES (:TRAN_ID, :TRAN_TYPE, :ACCT_NUM)
     */
    private String buildSql(String tableName, Set<String> columns) {

        if (columns.isEmpty()) {
            throw new RuntimeException("No columns to insert for table: " + tableName);
        }

        // Create comma-separated column list
        String columnList = String.join(", ", columns);

        // Create comma-separated named parameters (:TRAN_ID, :TRAN_TYPE, etc.)
        String valuesList = columns.stream()
                .map(col -> ":" + col)
                .collect(Collectors.joining(", "));

        // Build final SQL
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                columnList,
                valuesList
        );
    }

    /**
     * 🧹 Clear column cache (useful for testing)
     */
    public void clearCache() {
        dbColumnCache.clear();
    }
}