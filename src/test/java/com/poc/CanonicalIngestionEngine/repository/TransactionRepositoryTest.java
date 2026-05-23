package com.poc.CanonicalIngestionEngine.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Full unit-test suite for TransactionRepository.
 *
 * Truncation tests work by pre-seeding the instance field
 * "columnLengthCache" (Map<String, Map<String, Integer>>) via reflection
 * so that getColumnMaxLengths() returns the desired limits without hitting
 * the real DB.  The cache key is the upper-case table name parsed from SQL
 * (e.g. "INSERT INTO T ..." → cache key "T").
 */
@ExtendWith(MockitoExtension.class)
class TransactionRepositoryTest {

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    private TransactionRepository repository;

    @BeforeEach
    void setUp() {
        repository = new TransactionRepository(jdbcTemplate);
    }

    // =====================================================================
    // Reflection helpers
    // =====================================================================

    /**
     * Injects a column-length entry into the instance-level columnLengthCache.
     *
     * The cache structure is:  Map< TABLE_NAME , Map< COLUMN_NAME , maxLen > >
     * getColumnMaxLengths() uses computeIfAbsent, so pre-seeding the outer map
     * with the table key prevents any DB call and returns our injected limits.
     *
     * @param tableKey   upper-case table name as it appears after "INSERT INTO"
     *                   or "UPDATE" in the SQL string (e.g. "T", "SEND_TRANSACTIONS")
     * @param columnName upper-case column name
     * @param maxLen     maximum allowed length
     */
    @SuppressWarnings("unchecked")
    private void seedColumnMaxLength(
            String tableKey, String columnName, int maxLen) throws Exception {

        Field field = TransactionRepository.class
                .getDeclaredField("columnLengthCache");
        field.setAccessible(true);

        Map<String, Map<String, Integer>> cache =
                (Map<String, Map<String, Integer>>) field.get(repository);

        // computeIfAbsent: create the inner map for this table if not present
        cache.computeIfAbsent(
                tableKey.toUpperCase(),
                k -> new ConcurrentHashMap<>()
        ).put(columnName.toUpperCase(), maxLen);
    }

    /**
     * Removes a previously injected column-length entry from the cache.
     * Keeps tests independent of each other.
     */
    @SuppressWarnings("unchecked")
    private void clearTableCache(String tableKey) throws Exception {
        Field field = TransactionRepository.class
                .getDeclaredField("columnLengthCache");
        field.setAccessible(true);

        Map<String, Map<String, Integer>> cache =
                (Map<String, Map<String, Integer>>) field.get(repository);

        cache.remove(tableKey.toUpperCase());
    }

    /**
     * Invoke the private convertValue(Object) method directly so we can
     * assert catch-block behaviour independently of jdbcTemplate.update.
     */
    private Object invokeConvertValue(Object value) throws Exception {
        Method m = TransactionRepository.class
                .getDeclaredMethod("convertValue", Object.class);
        m.setAccessible(true);
        return m.invoke(repository, value);
    }

    // =====================================================================
    // INSERT — success / duplicate / generic failure
    // =====================================================================

    @Test
    @DisplayName("insert() calls jdbcTemplate.update with normalised params")
    void insert_success_callsJdbcUpdate() {
        when(jdbcTemplate.update(anyString(), any(Map.class))).thenReturn(1);

        assertDoesNotThrow(() ->
                repository.insert(
                        "INSERT INTO T (C) VALUES (:C)",
                        Map.of("C", "value")));

        verify(jdbcTemplate).update(anyString(), any(Map.class));
    }

    @Test
    @DisplayName("insert() silently ignores DuplicateKeyException")
    void insert_duplicateKey_silentlyIgnored() {
        when(jdbcTemplate.update(anyString(), any(Map.class)))
                .thenThrow(new DuplicateKeyException("dup"));

        assertDoesNotThrow(() ->
                repository.insert(
                        "INSERT INTO T (C) VALUES (:C)",
                        Map.of("C", "v")));
    }

    @Test
    @DisplayName("insert() wraps generic exception in IllegalStateException")
    void insert_genericException_throwsIllegalState() {
        when(jdbcTemplate.update(anyString(), any(Map.class)))
                .thenThrow(new RuntimeException("DB down"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> repository.insert(
                        "INSERT INTO T (C) VALUES (:C)",
                        Map.of("C", "v")));

        assertTrue(ex.getMessage().contains("Database INSERT failed"));
    }

    // =====================================================================
    // UPDATE — success / failure
    // =====================================================================

    @Test
    @DisplayName("update() succeeds without exception")
    void update_success() {
        when(jdbcTemplate.update(anyString(), any(Map.class))).thenReturn(1);

        assertDoesNotThrow(() ->
                repository.update("UPDATE T SET C=:c", Map.of("c", "val")));
    }

    @Test
    @DisplayName("update() wraps exception in IllegalStateException")
    void update_exception_throwsIllegalState() {
        when(jdbcTemplate.update(anyString(), any(Map.class)))
                .thenThrow(new RuntimeException("err"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> repository.update("UPDATE T SET C=:c", Map.of("c", "v")));

        assertTrue(ex.getMessage().contains("Database UPDATE failed"));
    }

    // =====================================================================
    // EXISTS
    // =====================================================================

    @Test
    @DisplayName("exists() returns true when count > 0")
    void exists_countGreaterThanZero_returnsTrue() {
        when(jdbcTemplate.queryForObject(
                anyString(), any(Map.class), eq(Integer.class)))
                .thenReturn(1);

        assertTrue(repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "T1"));
    }

    @Test
    @DisplayName("exists() returns false when count == 0")
    void exists_countZero_returnsFalse() {
        when(jdbcTemplate.queryForObject(
                anyString(), any(Map.class), eq(Integer.class)))
                .thenReturn(0);

        assertFalse(repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "T1"));
    }

    @Test
    @DisplayName("exists() returns false when count is null")
    void exists_countNull_returnsFalse() {
        when(jdbcTemplate.queryForObject(
                anyString(), any(Map.class), eq(Integer.class)))
                .thenReturn(null);

        assertFalse(repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "T1"));
    }

    // =====================================================================
    // FIND TRANSACTION
    // =====================================================================

    @Test
    @DisplayName("findTransaction() returns first row when result is non-empty")
    void findTransaction_nonEmpty_returnsFirstRow() {
        Map<String, Object> row = Map.of("TRAN_ID", "TXN-001");
        when(jdbcTemplate.queryForList(anyString(), any(Map.class)))
                .thenReturn(List.of(row));

        Map<String, Object> result = repository.findTransaction("TXN-001");

        assertNotNull(result);
        assertEquals("TXN-001", result.get("TRAN_ID"));
    }

    @Test
    @DisplayName("findTransaction() returns null when result is empty")
    void findTransaction_empty_returnsNull() {
        when(jdbcTemplate.queryForList(anyString(), any(Map.class)))
                .thenReturn(Collections.emptyList());

        assertNull(repository.findTransaction("TXN-404"));
    }

    // =====================================================================
    // FIND COLUMN VALUE
    // =====================================================================

    @Test
    @DisplayName("findColumnValue() returns first result when present")
    void findColumnValue_present_returnsValue() {
        when(jdbcTemplate.query(
                anyString(), any(Map.class), any(RowMapper.class)))
                .thenReturn(List.of("ACTIVE"));

        Object result = repository.findColumnValue(
                "SEND_TRANSACTIONS", "TRAN_ID", "TXN-001", "STATUS");

        assertEquals("ACTIVE", result);
    }

    @Test
    @DisplayName("findColumnValue() returns null when result is empty")
    void findColumnValue_empty_returnsNull() {
        when(jdbcTemplate.query(
                anyString(), any(Map.class), any(RowMapper.class)))
                .thenReturn(Collections.emptyList());

        assertNull(repository.findColumnValue(
                "SEND_TRANSACTIONS", "TRAN_ID", "TXN-404", "STATUS"));
    }

    // =====================================================================
    // FIND ALL RELATED DATA
    // =====================================================================

    @Test
    @DisplayName("findAllRelatedData() returns map with all four table keys")
    void findAllRelatedData_returnsFourKeys() {
        when(jdbcTemplate.queryForList(anyString(), any(Map.class)))
                .thenReturn(List.of());

        Map<String, List<Map<String, Object>>> result =
                repository.findAllRelatedData("TXN-001");

        assertEquals(4, result.size());
        assertTrue(result.containsKey("SEND_TRANSACTIONS"));
        assertTrue(result.containsKey("SEND_TRAN_DTL"));
        assertTrue(result.containsKey("SEND_RECIP_DTL"));
        assertTrue(result.containsKey("SEND_TRAN_ADDR_DTL"));
    }

    @Test
    @DisplayName("findAllRelatedData() issues exactly four SQL queries")
    void findAllRelatedData_issuesFourQueries() {
        when(jdbcTemplate.queryForList(anyString(), any(Map.class)))
                .thenReturn(List.of());

        repository.findAllRelatedData("TXN-001");

        verify(jdbcTemplate, times(4)).queryForList(anyString(), any(Map.class));
    }

    // =====================================================================
    // UPDATE STATUS
    // =====================================================================

    @Test
    @DisplayName("updateStatus() passes correct SQL and params")
    void updateStatus_correctSqlAndParams() {
        ArgumentCaptor<String> sqlCaptor =
                ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> paramsCaptor =
                ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(
                sqlCaptor.capture(), paramsCaptor.capture())).thenReturn(1);

        repository.updateStatus("TXN-001", "SETTLED");

        assertTrue(sqlCaptor.getValue().contains("UPDATE SEND_TRANSACTIONS"));
        assertEquals("SETTLED", paramsCaptor.getValue().get("status"));
        assertEquals("TXN-001", paramsCaptor.getValue().get("tranId"));
    }

    // =====================================================================
    // UPDATE COLUMN
    // =====================================================================

    @Test
    @DisplayName("updateColumn() builds correct SQL and passes value through when no cache entry")
    void updateColumn_correctSqlAndParams() {
        ArgumentCaptor<String> sqlCaptor =
                ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> paramsCaptor =
                ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(
                sqlCaptor.capture(), paramsCaptor.capture())).thenReturn(1);

        repository.updateColumn(
                "SEND_TRANSACTIONS", "TRAN_ID", "TXN-001", "STATUS", "SUCCESS");

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("UPDATE SEND_TRANSACTIONS"));
        assertTrue(sql.contains("STATUS"));

        Map<String, Object> params = paramsCaptor.getValue();
        assertEquals("SUCCESS", params.get("targetValue"));
        assertEquals("TXN-001", params.get("idValue"));
    }

    @Test
    @DisplayName("updateColumn() truncates string value when it exceeds cached column max length")
    void updateColumn_truncatesWhenExceedsMax() throws Exception {
        // Cache key = table name upper-cased; column key = targetColumn upper-cased
        seedColumnMaxLength("SEND_TRAN_DTL", "MSG_VERSION", 2);
        try {
            ArgumentCaptor<Map> paramsCaptor =
                    ArgumentCaptor.forClass(Map.class);
            when(jdbcTemplate.update(anyString(), paramsCaptor.capture()))
                    .thenReturn(1);

            // "v1" (len 2) fits; "v1_extended_value" (len 17) must be truncated to 2
            repository.updateColumn(
                    "SEND_TRAN_DTL", "TRAN_ID", "1349302531",
                    "MSG_VERSION", "v1_extended_value");

            Object stored = paramsCaptor.getValue().get("targetValue");
            assertInstanceOf(String.class, stored);
            assertEquals("v1", stored);   // truncated to maxLen=2
        } finally {
            clearTableCache("SEND_TRAN_DTL");
        }
    }

    @Test
    @DisplayName("updateColumn() passes value through unchanged when within cached max length")
    void updateColumn_noTruncateWhenWithinMax() throws Exception {
        seedColumnMaxLength("SEND_TRAN_DTL", "MSG_VERSION", 10);
        try {
            ArgumentCaptor<Map> paramsCaptor =
                    ArgumentCaptor.forClass(Map.class);
            when(jdbcTemplate.update(anyString(), paramsCaptor.capture()))
                    .thenReturn(1);

            repository.updateColumn(
                    "SEND_TRAN_DTL", "TRAN_ID", "123",
                    "MSG_VERSION", "v1");

            assertEquals("v1", paramsCaptor.getValue().get("targetValue"));
        } finally {
            clearTableCache("SEND_TRAN_DTL");
        }
    }

    @Test
    @DisplayName("updateColumn() passes non-String value through unchanged")
    void updateColumn_nonStringValue_passedThrough() {
        ArgumentCaptor<Map> paramsCaptor =
                ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), paramsCaptor.capture()))
                .thenReturn(1);

        repository.updateColumn(
                "SEND_TRANSACTIONS", "TRAN_ID", "TXN-001", "AMOUNT", 9999);

        assertEquals(9999, paramsCaptor.getValue().get("targetValue"));
    }

    // =====================================================================
    // COLUMN EXISTS
    // =====================================================================

    @Test
    @DisplayName("columnExists() returns true when count is 1")
    void columnExists_returnsTrue() {
        when(jdbcTemplate.queryForObject(
                anyString(), any(Map.class), eq(Integer.class)))
                .thenReturn(1);

        assertTrue(repository.columnExists("SEND_TRANSACTIONS", "STATUS"));
    }

    @Test
    @DisplayName("columnExists() returns false when count is 0")
    void columnExists_zero_returnsFalse() {
        when(jdbcTemplate.queryForObject(
                anyString(), any(Map.class), eq(Integer.class)))
                .thenReturn(0);

        assertFalse(repository.columnExists("SEND_TRANSACTIONS", "BAD_COL"));
    }

    @Test
    @DisplayName("columnExists() returns false when count is null")
    void columnExists_null_returnsFalse() {
        when(jdbcTemplate.queryForObject(
                anyString(), any(Map.class), eq(Integer.class)))
                .thenReturn(null);

        assertFalse(repository.columnExists("SEND_TRANSACTIONS", "BAD_COL"));
    }

    // =====================================================================
    // VALUE CONVERSION — happy-path
    // =====================================================================

    @Test
    @DisplayName("convertValue(): 'yyyy-MM-ddTHH:mm:ss' → Timestamp")
    void convertValue_isoDateTimeWithT_returnsTimestamp() throws Exception {
        Object result = invokeConvertValue("2025-06-15T10:30:00");
        assertInstanceOf(Timestamp.class, result);
    }

    @Test
    @DisplayName("convertValue(): 'yyyy-MM-dd HH:mm:ss' → Timestamp")
    void convertValue_standardTimestamp_returnsTimestamp() throws Exception {
        Object result = invokeConvertValue("2025-06-15 10:30:00");
        assertInstanceOf(Timestamp.class, result);
    }

    @Test
    @DisplayName("convertValue(): 'yyyy-MM-dd' → java.sql.Date")
    void convertValue_dateOnly_returnsSqlDate() throws Exception {
        Object result = invokeConvertValue("1990-01-01");
        assertInstanceOf(Date.class, result);
    }

    @Test
    @DisplayName("convertValue(): plain string passes through unchanged")
    void convertValue_plainString_passesThrough() throws Exception {
        Object result = invokeConvertValue("John Doe");
        assertEquals("John Doe", result);
    }

    @Test
    @DisplayName("convertValue(): Integer passes through unchanged")
    void convertValue_integer_passesThrough() throws Exception {
        Object result = invokeConvertValue(42);
        assertEquals(42, result);
    }

    @Test
    @DisplayName("convertValue(): null passes through unchanged")
    void convertValue_null_passesThrough() throws Exception {
        Object result = invokeConvertValue(null);
        assertNull(result);
    }

    // =====================================================================
    // VALUE CONVERSION — catch block
    //
    // Month "00" satisfies \d{2} in every regex but is rejected by
    // Date.valueOf() / Timestamp.valueOf() on all JVMs (months are 1-based).
    // The catch block logs at DEBUG and returns the original String.
    // =====================================================================

    @Test
    @DisplayName("convertValue() catch: date-only with month=00 returns original String")
    void convertValue_catch_dateOnlyMonthZero_returnsOriginalString()
            throws Exception {
        String input = "2025-00-01";
        Object result = invokeConvertValue(input);
        assertInstanceOf(String.class, result);
        assertEquals(input, result);
    }

    @Test
    @DisplayName("convertValue() catch: T-separated timestamp with month=00 returns original String")
    void convertValue_catch_timestampT_monthZero_returnsOriginalString()
            throws Exception {
        String input = "2025-00-01T10:00:00";
        Object result = invokeConvertValue(input);
        assertInstanceOf(String.class, result);
        assertEquals(input, result);
    }

    @Test
    @DisplayName("convertValue() catch: space-separated timestamp with month=00 returns original String")
    void convertValue_catch_timestampSpace_monthZero_returnsOriginalString()
            throws Exception {
        String input = "2025-00-01 10:00:00";
        Object result = invokeConvertValue(input);
        assertInstanceOf(String.class, result);
        assertEquals(input, result);
    }

    // =====================================================================
    // normalizeParams() — type-conversion pipeline via insert()
    // =====================================================================

    @Test
    @DisplayName("normalizeParams() converts 'yyyy-MM-ddTHH:mm:ss' to Timestamp via insert()")
    void normalizeParams_isoDateTimeWithT_convertedToTimestamp() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        repository.insert(
                "INSERT INTO T (TS) VALUES (:TS)",
                Map.of("TS", "2025-06-15T10:30:00"));

        assertInstanceOf(Timestamp.class, captor.getValue().get("TS"));
    }

    @Test
    @DisplayName("normalizeParams() converts 'yyyy-MM-dd HH:mm:ss' to Timestamp via insert()")
    void normalizeParams_standardTimestamp_convertedToTimestamp() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        repository.insert(
                "INSERT INTO T (TS) VALUES (:TS)",
                Map.of("TS", "2025-06-15 10:30:00"));

        assertInstanceOf(Timestamp.class, captor.getValue().get("TS"));
    }

    @Test
    @DisplayName("normalizeParams() converts 'yyyy-MM-dd' to java.sql.Date via insert()")
    void normalizeParams_dateOnlyString_convertedToSqlDate() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        repository.insert(
                "INSERT INTO T (DOB) VALUES (:DOB)",
                Map.of("DOB", "1990-01-01"));

        assertInstanceOf(Date.class, captor.getValue().get("DOB"));
    }

    @Test
    @DisplayName("normalizeParams() leaves plain non-date strings unchanged")
    void normalizeParams_plainString_passedThrough() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        repository.insert(
                "INSERT INTO T (NAME) VALUES (:NAME)",
                Map.of("NAME", "John Doe"));

        assertEquals("John Doe", captor.getValue().get("NAME"));
    }

    @Test
    @DisplayName("normalizeParams() passes Integer values through unchanged")
    void normalizeParams_integerValue_passedThrough() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        repository.insert(
                "INSERT INTO T (CNT) VALUES (:CNT)",
                Map.of("CNT", 42));

        assertEquals(42, captor.getValue().get("CNT"));
    }

    @Test
    @DisplayName("normalizeParams() passes null values through unchanged")
    void normalizeParams_nullValue_passedThrough() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        Map<String, Object> params = new HashMap<>();
        params.put("COL", null);

        repository.insert("INSERT INTO T (COL) VALUES (:COL)", params);

        assertTrue(captor.getValue().containsKey("COL"));
        assertNull(captor.getValue().get("COL"));
    }

    // =====================================================================
    // TRUNCATION BRANCH — normalizeParams() via insert()
    //
    // The SQL "INSERT INTO T ..." causes extractTableName() to return "T".
    // We seed columnLengthCache with key "T" and the desired column limits.
    // =====================================================================

    @Test
    @DisplayName("normalizeParams() truncates string when value exceeds cached column max length")
    void normalizeParams_stringExceedsMaxLength_isTruncated() throws Exception {
        seedColumnMaxLength("T", "SHORT_COL", 5);
        try {
            ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
            when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

            repository.insert(
                    "INSERT INTO T (SHORT_COL) VALUES (:SHORT_COL)",
                    Map.of("SHORT_COL", "ABCDEFGHIJ"));

            Object result = captor.getValue().get("SHORT_COL");
            assertInstanceOf(String.class, result);
            assertEquals("ABCDE", result);
        } finally {
            clearTableCache("T");
        }
    }

    @Test
    @DisplayName("normalizeParams() does NOT truncate when string is within cached column max length")
    void normalizeParams_stringWithinMaxLength_notTruncated() throws Exception {
        seedColumnMaxLength("T", "SHORT_COL", 20);
        try {
            ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
            when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

            repository.insert(
                    "INSERT INTO T (SHORT_COL) VALUES (:SHORT_COL)",
                    Map.of("SHORT_COL", "HELLO"));

            assertEquals("HELLO", captor.getValue().get("SHORT_COL"));
        } finally {
            clearTableCache("T");
        }
    }

    @Test
    @DisplayName("normalizeParams() does NOT truncate when column has no cache entry")
    void normalizeParams_noMaxLengthCached_passedThroughUnchanged() {
        // No seedColumnMaxLength call — cache is empty for this table/column
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        String longVal = "A".repeat(500);
        repository.insert(
                "INSERT INTO T (SOME_COL) VALUES (:SOME_COL)",
                Map.of("SOME_COL", longVal));

        assertEquals(longVal, captor.getValue().get("SOME_COL"));
    }

    @Test
    @DisplayName("normalizeParams() truncates at boundary (length == maxLen + 1)")
    void normalizeParams_stringExceedsByOne_isTruncated() throws Exception {
        seedColumnMaxLength("T", "EDGE_COL", 3);
        try {
            ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
            when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

            repository.insert(
                    "INSERT INTO T (EDGE_COL) VALUES (:EDGE_COL)",
                    Map.of("EDGE_COL", "ABCD"));   // len 4, max 3

            assertEquals("ABC", captor.getValue().get("EDGE_COL"));
        } finally {
            clearTableCache("T");
        }
    }

    @Test
    @DisplayName("normalizeParams() does not truncate string at exact max length boundary")
    void normalizeParams_stringExactlyAtMaxLength_notTruncated() throws Exception {
        seedColumnMaxLength("T", "EXACT_COL", 5);
        try {
            ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
            when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

            repository.insert(
                    "INSERT INTO T (EXACT_COL) VALUES (:EXACT_COL)",
                    Map.of("EXACT_COL", "HELLO"));  // len 5, max 5 — exact fit

            assertEquals("HELLO", captor.getValue().get("EXACT_COL"));
        } finally {
            clearTableCache("T");
        }
    }

    // =====================================================================
    // Multiple params in a single call
    // =====================================================================

    @Test
    @DisplayName("insert() correctly normalises multiple params in one call")
    void insert_multipleParams_allNormalised() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        when(jdbcTemplate.update(anyString(), captor.capture())).thenReturn(1);

        Map<String, Object> params = new HashMap<>();
        params.put("DT",    "2025-01-15");
        params.put("TS",    "2025-01-15T08:00:00");
        params.put("AMT",   100.0);
        params.put("LABEL", "Hello");

        repository.insert(
                "INSERT INTO T (DT,TS,AMT,LABEL) VALUES (:DT,:TS,:AMT,:LABEL)",
                params);

        Map<?, ?> normalised = captor.getValue();
        assertInstanceOf(Date.class,      normalised.get("DT"));
        assertInstanceOf(Timestamp.class, normalised.get("TS"));
        assertEquals(100.0,              normalised.get("AMT"));
        assertEquals("Hello",            normalised.get("LABEL"));
    }
}