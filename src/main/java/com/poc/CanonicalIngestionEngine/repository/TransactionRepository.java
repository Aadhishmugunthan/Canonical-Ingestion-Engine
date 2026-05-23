package com.poc.CanonicalIngestionEngine.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Repository
public class TransactionRepository {

    private static final Logger log =
            LoggerFactory.getLogger(TransactionRepository.class);

    // =====================================================
    // CACHE: tableName -> { columnName -> maxLength }
    // Populated lazily from USER_TAB_COLUMNS on first use.
    // =====================================================
    private final Map<String, Map<String, Integer>> columnLengthCache =
            new ConcurrentHashMap<>();

    // Regex to extract table name from INSERT / UPDATE SQL
    private static final Pattern INSERT_TABLE_PATTERN =
            Pattern.compile(
                    "(?i)INSERT\\s+INTO\\s+(\\w+)",
                    Pattern.CASE_INSENSITIVE
            );

    private static final Pattern UPDATE_TABLE_PATTERN =
            Pattern.compile(
                    "(?i)UPDATE\\s+(\\w+)",
                    Pattern.CASE_INSENSITIVE
            );

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public TransactionRepository(
            NamedParameterJdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // =====================================================
    // INSERT
    // =====================================================

    public void insert(
            String sql,
            Map<String, Object> params
    ) {
        try {
            String tableName = extractTableName(sql, INSERT_TABLE_PATTERN);
            Map<String, Object> normalized = normalizeParams(params, tableName);
            jdbcTemplate.update(sql, normalized);
        }
        catch (DuplicateKeyException e) {
            log.warn(
                    "Duplicate record detected. Insert skipped. SQL={} PARAMS={}",
                    sql,
                    params
            );
        }
        catch (Exception e) {
            log.error(
                    "Database INSERT failed | sql={} | params={}",
                    sql,
                    params,
                    e
            );
            throw new IllegalStateException(
                    "Database INSERT failed for SQL: " + sql,
                    e
            );
        }
    }

    // =====================================================
    // UPDATE
    // =====================================================

    public void update(
            String sql,
            Map<String, Object> params
    ) {
        try {
            String tableName = extractTableName(sql, UPDATE_TABLE_PATTERN);
            Map<String, Object> normalized = normalizeParams(params, tableName);
            jdbcTemplate.update(sql, normalized);
        }
        catch (Exception e) {
            log.error(
                    "Database UPDATE failed | sql={} | params={}",
                    sql,
                    params,
                    e
            );
            throw new IllegalStateException(
                    "Database UPDATE failed for SQL: " + sql,
                    e
            );
        }
    }

    // =====================================================
    // EXISTS
    // =====================================================

    public boolean exists(
            String tableName,
            String columnName,
            String value
    ) {
        String sql =
                "SELECT COUNT(1) FROM "
                        + tableName
                        + " WHERE "
                        + columnName
                        + " = :value";

        Map<String, Object> params = new HashMap<>();
        params.put("value", value);

        Integer count =
                jdbcTemplate.queryForObject(
                        sql,
                        params,
                        Integer.class
                );

        return count != null && count > 0;
    }

    // =====================================================
    // FETCH MAIN TRANSACTION
    // =====================================================

    public Map<String, Object> findTransaction(
            String tranId
    ) {
        String sql = """
                SELECT *
                FROM SEND_TRANSACTIONS
                WHERE TRAN_ID = :tranId
                """;

        Map<String, Object> params = new HashMap<>();
        params.put("tranId", tranId);

        List<Map<String, Object>> result =
                jdbcTemplate.queryForList(sql, params);

        if (result.isEmpty()) {
            return null;
        }

        return result.get(0);
    }

    // =====================================================
    // FETCH SINGLE COLUMN VALUE
    // =====================================================

    public Object findColumnValue(
            String tableName,
            String idColumn,
            String idValue,
            String targetColumn
    ) {
        String sql =
                "SELECT "
                        + targetColumn
                        + " FROM "
                        + tableName
                        + " WHERE "
                        + idColumn
                        + " = :idValue";

        Map<String, Object> params = new HashMap<>();
        params.put("idValue", idValue);

        List<Object> result =
                jdbcTemplate.query(
                        sql,
                        params,
                        (rs, rowNum) -> rs.getObject(1)
                );

        if (result.isEmpty()) {
            return null;
        }

        return result.get(0);
    }

    // =====================================================
    // FETCH RELATED DATA
    // =====================================================

    public Map<String, List<Map<String, Object>>>
    findAllRelatedData(String tranId) {

        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        Map<String, Object> params = new HashMap<>();
        params.put("tranId", tranId);

        result.put(
                "SEND_TRANSACTIONS",
                jdbcTemplate.queryForList(
                        """
                        SELECT *
                        FROM SEND_TRANSACTIONS
                        WHERE TRAN_ID = :tranId
                        """,
                        params
                )
        );

        result.put(
                "SEND_TRAN_DTL",
                jdbcTemplate.queryForList(
                        """
                        SELECT *
                        FROM SEND_TRAN_DTL
                        WHERE TRAN_ID = :tranId
                        """,
                        params
                )
        );

        result.put(
                "SEND_RECIP_DTL",
                jdbcTemplate.queryForList(
                        """
                        SELECT *
                        FROM SEND_RECIP_DTL
                        WHERE TRAN_ID = :tranId
                        """,
                        params
                )
        );

        result.put(
                "SEND_TRAN_ADDR_DTL",
                jdbcTemplate.queryForList(
                        """
                        SELECT *
                        FROM SEND_TRAN_ADDR_DTL
                        WHERE TRAN_ID = :tranId
                        """,
                        params
                )
        );

        return result;
    }

    // =====================================================
    // UPDATE STATUS
    // =====================================================

    public void updateStatus(
            String tranId,
            String status
    ) {
        String sql = """
                UPDATE SEND_TRANSACTIONS
                SET STATUS = :status
                WHERE TRAN_ID = :tranId
                """;

        Map<String, Object> params = new HashMap<>();
        params.put("status", status);
        params.put("tranId", tranId);

        Map<String, Object> normalized =
                normalizeParams(params, "SEND_TRANSACTIONS");

        jdbcTemplate.update(sql, normalized);
    }

    // =====================================================
    // UPDATE SINGLE COLUMN
    // =====================================================

    public void updateColumn(
            String tableName,
            String idColumn,
            String idValue,
            String targetColumn,
            Object targetValue
    ) {
        String sql =
                "UPDATE "
                        + tableName
                        + " SET "
                        + targetColumn
                        + " = :targetValue "
                        + " WHERE "
                        + idColumn
                        + " = :idValue";

        // Truncate targetValue to the actual DB column max length
        // so ORA-12899 never fires on single-column updates either.
        Object safeValue = targetValue;
        if (targetValue instanceof String str) {
            Map<String, Integer> lengths =
                    getColumnMaxLengths(tableName);
            Integer maxLen =
                    lengths.get(targetColumn.toUpperCase());
            if (maxLen != null && str.length() > maxLen) {
                log.warn(
                        "Truncating updateColumn column={} table={} actual={} max={}",
                        targetColumn,
                        tableName,
                        str.length(),
                        maxLen
                );
                safeValue = str.substring(0, maxLen);
            }
        }

        Map<String, Object> params = new HashMap<>();
        params.put("targetValue", safeValue);
        params.put("idValue", idValue);

        jdbcTemplate.update(sql, params);
    }

    // =====================================================
    // CHECK COLUMN EXISTS
    // =====================================================

    public boolean columnExists(
            String tableName,
            String columnName
    ) {
        String sql = """
                SELECT COUNT(1)
                FROM USER_TAB_COLUMNS
                WHERE TABLE_NAME = :tableName
                AND COLUMN_NAME = :columnName
                """;

        Map<String, Object> params = new HashMap<>();
        params.put("tableName", tableName.toUpperCase());
        params.put("columnName", columnName.toUpperCase());

        Integer count =
                jdbcTemplate.queryForObject(
                        sql,
                        params,
                        Integer.class
                );

        return count != null && count > 0;
    }

    // =====================================================
    // FETCH COLUMN MAX LENGTHS FROM DB (cached per table)
    // =====================================================

    /**
     * Queries USER_TAB_COLUMNS for all VARCHAR2/CHAR column lengths
     * for the given table. Results are cached in-memory so the DB
     * is only hit once per table per application lifetime.
     *
     * @param tableName the Oracle table name (case-insensitive)
     * @return map of COLUMN_NAME (upper-case) -> DATA_LENGTH
     */
    private Map<String, Integer> getColumnMaxLengths(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return new HashMap<>();
        }

        String key = tableName.toUpperCase();

        return columnLengthCache.computeIfAbsent(key, t -> {
            String sql = """
                    SELECT COLUMN_NAME, DATA_LENGTH
                    FROM USER_TAB_COLUMNS
                    WHERE TABLE_NAME = :tableName
                    AND DATA_TYPE IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR')
                    """;

            Map<String, Object> params = new HashMap<>();
            params.put("tableName", t);

            Map<String, Integer> lengths = new HashMap<>();

            jdbcTemplate.query(sql, params, rs -> {
                lengths.put(
                        rs.getString("COLUMN_NAME").toUpperCase(),
                        rs.getInt("DATA_LENGTH")
                );
            });

            log.debug(
                    "Loaded column max lengths for table={}: {}",
                    t,
                    lengths
            );

            return lengths;
        });
    }

    // =====================================================
    // EXTRACT TABLE NAME FROM SQL
    // =====================================================

    private String extractTableName(String sql, Pattern pattern) {
        if (sql == null) return null;
        Matcher matcher = pattern.matcher(sql.replaceAll("\\s+", " ").trim());
        if (matcher.find()) {
            return matcher.group(1).toUpperCase();
        }
        return null;
    }

    // =====================================================
    // PARAM NORMALIZER
    // Now uses actual DB column max lengths per table.
    // Falls back to no truncation if table/column not found.
    // =====================================================

    private Map<String, Object> normalizeParams(
            Map<String, Object> params,
            String tableName
    ) {
        Map<String, Integer> columnLengths =
                getColumnMaxLengths(tableName);

        Map<String, Object> normalized = new HashMap<>();

        for (Map.Entry<String, Object> entry : params.entrySet()) {

            Object value = convertValue(entry.getValue());

            if (value instanceof String str) {
                String columnKey = entry.getKey().toUpperCase();
                Integer maxLen   = columnLengths.get(columnKey);

                if (maxLen != null && str.length() > maxLen) {
                    log.warn(
                            "Truncating param column={} table={} actual={} max={}",
                            entry.getKey(),
                            tableName,
                            str.length(),
                            maxLen
                    );
                    value = str.substring(0, maxLen);
                }
                // If column not found in metadata (e.g. non-string column
                // passed as String, or table name could not be parsed),
                // the value is passed through as-is — no silent data loss.
            }

            normalized.put(entry.getKey(), value);
        }

        return normalized;
    }

    // =====================================================
    // VALUE CONVERTER
    // =====================================================

    private Object convertValue(Object value) {

        if (!(value instanceof String str)) {
            return value;
        }

        try {
            if (str.matches(
                    "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}"
            )) {
                return Timestamp.valueOf(str.replace("T", " "));
            }

            if (str.matches(
                    "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"
            )) {
                return Timestamp.valueOf(str);
            }

            if (str.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return Date.valueOf(str);
            }
        }
        catch (Exception e) {
            log.debug(
                    "Value conversion failed for value={}",
                    value
            );
            return value;
        }

        return value;
    }
}