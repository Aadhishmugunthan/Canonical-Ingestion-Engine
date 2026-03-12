package com.poc.CanonicalIngestionEngine.repository;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Repository
public class TransactionRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public TransactionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // ================= INSERT =================

    public void insert(String sql, Map<String, Object> params) {

        try {

            Map<String, Object> normalizedParams = normalizeParams(params);

            jdbcTemplate.update(sql, normalizedParams);

        } catch (DuplicateKeyException e) {

            // Duplicate key is allowed → ignore

        } catch (Exception e) {

            throw new IllegalStateException(
                    "Database INSERT failed for SQL: " + sql, e);
        }
    }

    // ================= UPDATE =================

    public void update(String sql, Map<String, Object> params) {

        try {

            Map<String, Object> normalizedParams = normalizeParams(params);

            jdbcTemplate.update(sql, normalizedParams);

        } catch (Exception e) {

            throw new IllegalStateException(
                    "Database UPDATE failed for SQL: " + sql, e);
        }
    }

    // ================= PARAM NORMALIZER =================

    private Map<String, Object> normalizeParams(Map<String, Object> params) {

        Map<String, Object> normalized = new HashMap<>();

        for (Map.Entry<String, Object> entry : params.entrySet()) {

            Object value = convertValue(entry.getValue());

            normalized.put(entry.getKey(), value);
        }

        return normalized;
    }

    // ================= VALUE CONVERTER =================

    private Object convertValue(Object value) {

        if (!(value instanceof String str)) {
            return value;
        }

        try {

            // ISO timestamp
            if (str.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")) {
                return Timestamp.valueOf(str.replace("T", " "));
            }

            // Timestamp without T
            if (str.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                return Timestamp.valueOf(str);
            }

            // Date only
            if (str.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return Date.valueOf(str);
            }

        } catch (Exception e) {

            // If conversion fails, keep original value
            return value;
        }

        return value;
    }

    // ================= EXISTS CHECK =================

    public boolean exists(String tableName, String columnName, String value) {

        try {

            String sql = "SELECT COUNT(1) FROM " + tableName +
                    " WHERE " + columnName + " = :value";

            Map<String, Object> params = new HashMap<>();
            params.put("value", value);

            Integer count = jdbcTemplate.queryForObject(sql, params, Integer.class);

            return count != null && count > 0;

        } catch (Exception e) {

            throw new IllegalStateException(
                    "Exists check failed for table: " + tableName, e);
        }
    }

    // ================= EXISTS WITH TYPE =================

    public boolean existsWithType(String tableName,
                                  String idCol,
                                  String idVal,
                                  String typeCol,
                                  String typeVal) {

        try {

            String sql = "SELECT COUNT(1) FROM " + tableName +
                    " WHERE " + idCol + " = :idVal" +
                    " AND " + typeCol + " = :typeVal";

            Map<String, Object> params = new HashMap<>();
            params.put("idVal", idVal);
            params.put("typeVal", typeVal);

            Integer count = jdbcTemplate.queryForObject(sql, params, Integer.class);

            return count != null && count > 0;

        } catch (Exception e) {

            throw new IllegalStateException(
                    "ExistsWithType check failed for table: " + tableName, e);
        }
    }
}