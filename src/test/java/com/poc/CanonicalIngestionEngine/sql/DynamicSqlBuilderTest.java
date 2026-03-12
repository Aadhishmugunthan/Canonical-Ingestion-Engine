package com.poc.CanonicalIngestionEngine.sql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DynamicSqlBuilderTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private DynamicSqlBuilder sqlBuilder;

    // =========================================================
    // SETUP
    // =========================================================
    @BeforeEach
    void setUp() {
        sqlBuilder = new DynamicSqlBuilder(jdbcTemplate);
    }

    // =========================================================
    // HELPER: mock DB columns
    // =========================================================
    private void mockDbColumns(String upperCaseTableName, List<String> columns) {
        when(jdbcTemplate.queryForList(
                anyString(),
                eq(String.class),
                eq(upperCaseTableName)
        )).thenReturn(columns);
    }

    // =========================================================
    // INSERT TESTS
    // =========================================================

    @Test
    @DisplayName("INSERT: all mapped columns exist in DB")
    void testInsert_AllColumnsExist() {
        mockDbColumns("SEND_TRANSACTIONS",
                Arrays.asList("TRAN_ID", "TRAN_TYPE", "ACCT_NUM"));

        String sql = sqlBuilder.buildInsertSql(
                "SEND_TRANSACTIONS",
                new HashSet<>(Arrays.asList("TRAN_ID", "TRAN_TYPE", "ACCT_NUM")),
                false
        );

        assertNotNull(sql);
        assertTrue(sql.contains("INSERT INTO SEND_TRANSACTIONS"));
        assertTrue(sql.contains("TRAN_ID"));
        assertTrue(sql.contains(":TRAN_ID"));
    }

    @Test
    @DisplayName("INSERT: filters non DB columns")
    void testInsert_FiltersNonDbColumns() {
        mockDbColumns("SEND_TRANSACTIONS",
                Arrays.asList("TRAN_ID", "TRAN_TYPE"));

        String sql = sqlBuilder.buildInsertSql(
                "SEND_TRANSACTIONS",
                new HashSet<>(Arrays.asList("TRAN_ID", "TRAN_TYPE", "NOT_IN_DB")),
                false
        );

        assertFalse(sql.contains("NOT_IN_DB"));
        assertTrue(sql.contains("TRAN_ID"));
    }

    @Test
    @DisplayName("INSERT: autoGenerateId adds ID if exists in DB")
    void testInsert_AutoGenerateId() {
        mockDbColumns("SEND_RECIP_DTL",
                Arrays.asList("ID", "TRAN_ID"));

        String sql = sqlBuilder.buildInsertSql(
                "SEND_RECIP_DTL",
                new HashSet<>(Arrays.asList("TRAN_ID")),
                true
        );

        assertTrue(sql.contains("ID"));
        assertTrue(sql.contains(":ID"));
    }

    @Test
    @DisplayName("INSERT: throws when no matching columns")
    void testInsert_ThrowsWhenNoMatchingColumns() {
        mockDbColumns("SEND_TRANSACTIONS",
                Arrays.asList("TRAN_ID"));

        // Fix: Prepare all arguments BEFORE the lambda so only the single
        // throwing invocation is inside assertThrows (SonarQube Medium S5778)
        Set<String> nonExistentColumns = new HashSet<>(Arrays.asList("NOT_EXIST"));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                sqlBuilder.buildInsertSql("SEND_TRANSACTIONS", nonExistentColumns, false)
        );

        assertTrue(ex.getMessage().contains("No valid columns"));
    }

    @Test
    @DisplayName("INSERT: caching works")
    void testInsert_CacheWorks() {
        mockDbColumns("SEND_TRANSACTIONS",
                Arrays.asList("TRAN_ID"));

        sqlBuilder.buildInsertSql("SEND_TRANSACTIONS",
                new HashSet<>(Arrays.asList("TRAN_ID")), false);

        sqlBuilder.buildInsertSql("SEND_TRANSACTIONS",
                new HashSet<>(Arrays.asList("TRAN_ID")), false);

        verify(jdbcTemplate, times(1))
                .queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS"));
    }

    // =========================================================
    // UPDATE TESTS
    // =========================================================

    @Test
    @DisplayName("UPDATE: basic SQL")
    void testUpdate_BasicSql() {
        mockDbColumns("SEND_TRANSACTIONS",
                Arrays.asList("TRAN_ID", "STATUS"));

        String sql = sqlBuilder.buildUpdateSql(
                "SEND_TRANSACTIONS",
                new LinkedHashSet<>(Arrays.asList("TRAN_ID", "STATUS")),
                "TRAN_ID"
        );

        assertNotNull(sql);
        assertTrue(sql.contains("UPDATE SEND_TRANSACTIONS SET"));
        assertTrue(sql.contains("STATUS = :STATUS"));
        assertTrue(sql.contains("WHERE TRAN_ID = :TRAN_ID"));
    }

    @Test
    @DisplayName("UPDATE: filters non DB columns")
    void testUpdate_FiltersNonDbColumns() {
        mockDbColumns("SEND_TRANSACTIONS",
                Arrays.asList("TRAN_ID", "STATUS"));

        String sql = sqlBuilder.buildUpdateSql(
                "SEND_TRANSACTIONS",
                new LinkedHashSet<>(Arrays.asList("TRAN_ID", "STATUS", "BAD")),
                "TRAN_ID"
        );

        assertFalse(sql.contains("BAD"));
        assertTrue(sql.contains("STATUS = :STATUS"));
    }

    @Test
    @DisplayName("UPDATE: caching works")
    void testUpdate_UsesCache() {
        mockDbColumns("SEND_TRANSACTIONS",
                Arrays.asList("TRAN_ID", "STATUS"));

        sqlBuilder.buildInsertSql("SEND_TRANSACTIONS",
                new LinkedHashSet<>(Arrays.asList("TRAN_ID", "STATUS")), false);

        sqlBuilder.buildUpdateSql("SEND_TRANSACTIONS",
                new LinkedHashSet<>(Arrays.asList("TRAN_ID", "STATUS")), "TRAN_ID");

        verify(jdbcTemplate, times(1))
                .queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS"));
    }
}