package com.poc.CanonicalIngestionEngine.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class DynamicSqlBuilder {

    private static final Logger log =
            LoggerFactory.getLogger(DynamicSqlBuilder.class);

    private final JdbcTemplate jdbcTemplate;

    // Thread-safe DB column cache
    private final Map<String, Set<String>> dbColumnCache =
            new ConcurrentHashMap<>();

    public DynamicSqlBuilder(
            JdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // =========================================================
    // INSERT SQL BUILDER
    // =========================================================

    public String buildInsertSql(
            String tableName,
            Set<String> mappedColumns,
            boolean autoGenerateId
    ) {

        log.debug(
                "Building INSERT SQL for table={}",
                tableName
        );

        log.debug(
                "Incoming mapped columns={}",
                mappedColumns
        );

        Set<String> dbColumns =
                getDbColumns(tableName);

        log.debug(
                "DB columns={}",
                dbColumns
        );

        Set<String> finalColumns =
                new LinkedHashSet<>(mappedColumns);

        // Add ID if needed

        if (
                autoGenerateId &&
                        dbColumns.contains("ID")
        ) {

            finalColumns.add("ID");

            log.debug(
                    "Added ID column (autoGenerateId enabled)"
            );
        }

        // Keep only valid DB columns

        finalColumns.retainAll(dbColumns);

        if (finalColumns.isEmpty()) {

            throw new IllegalStateException(
                    "No valid columns to insert into table: "
                            + tableName
            );
        }

        log.debug(
                "Final insert columns={}",
                finalColumns
        );

        String sql =
                buildInsert(tableName, finalColumns);

        log.debug(
                "Generated INSERT SQL={}",
                sql
        );

        return sql;
    }

    // =========================================================
    // UPDATE SQL BUILDER
    // =========================================================

    public String buildUpdateSql(
            String tableName,
            Set<String> mappedColumns,
            String whereColumn
    ) {

        log.debug(
                "Building UPDATE SQL for table={}",
                tableName
        );

        Set<String> dbColumns =
                getDbColumns(tableName);

        Set<String> finalColumns =
                new LinkedHashSet<>(mappedColumns);

        finalColumns.retainAll(dbColumns);

        if (!finalColumns.contains(whereColumn)) {

            finalColumns.add(whereColumn);
        }

        if (finalColumns.isEmpty()) {

            throw new IllegalStateException(
                    "No valid columns available for update: "
                            + tableName
            );
        }

        StringBuilder sql =
                new StringBuilder();

        sql.append("UPDATE ")
                .append(tableName)
                .append(" SET ");

        boolean first = true;

        for (String col : finalColumns) {

            if (col.equalsIgnoreCase(whereColumn)) {
                continue;
            }

            if (!first) {
                sql.append(", ");
            }

            sql.append(col)
                    .append(" = :")
                    .append(col);

            first = false;
        }

        sql.append(" WHERE ")
                .append(whereColumn)
                .append(" = :")
                .append(whereColumn);

        String finalSql = sql.toString();

        log.debug(
                "Generated UPDATE SQL={}",
                finalSql
        );

        return finalSql;
    }

    // =========================================================
    // FETCH DB COLUMNS FROM ORACLE
    // =========================================================

    private Set<String> getDbColumns(
            String tableName
    ) {

        return dbColumnCache.computeIfAbsent(
                tableName,
                key -> {

                    String sql = """
                            SELECT COLUMN_NAME
                            FROM USER_TAB_COLUMNS
                            WHERE TABLE_NAME = ?
                            """;

                    List<String> columns =
                            jdbcTemplate.queryForList(
                                    sql,
                                    String.class,
                                    key.toUpperCase()
                            );

                    Set<String> columnSet =
                            new HashSet<>(columns);

                    log.debug(
                            "Fetched and cached columns for {}: {}",
                            key,
                            columnSet
                    );

                    return columnSet;
                }
        );
    }

    // =========================================================
    // BUILD INSERT SQL STRING
    // =========================================================

    private String buildInsert(
            String tableName,
            Set<String> columns
    ) {

        String columnList =
                String.join(", ", columns);

        String valuesList =
                columns.stream()
                        .map(col -> ":" + col)
                        .collect(Collectors.joining(", "));

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                columnList,
                valuesList
        );
    }

    // =========================================================
    // CLEAR CACHE
    // =========================================================

    public void clearCache() {

        dbColumnCache.clear();

        log.info(
                "DB column cache cleared"
        );
    }
}