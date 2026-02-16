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

/**
 * Test cases for DynamicSqlBuilder
 */
@ExtendWith(MockitoExtension.class)
class DynamicSqlBuilderTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private DynamicSqlBuilder sqlBuilder;

    @BeforeEach
    void setUp() {
        sqlBuilder = new DynamicSqlBuilder();
        // Use reflection to inject the mock
        try {
            var field = DynamicSqlBuilder.class.getDeclaredField("jdbcTemplate");
            field.setAccessible(true);
            field.set(sqlBuilder, jdbcTemplate);
        } catch (Exception e) {
            fail("Failed to inject mock: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Should build INSERT SQL with all columns that exist in DB")
    void testBuildInsertSqlAllColumnsExist() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList(
                "TRAN_ID", "TRAN_TYPE", "ACCT_NUM"
        ));

        // Mock DB columns
        List<String> dbColumns = Arrays.asList(
                "TRAN_ID", "TRAN_TYPE", "ACCT_NUM", "EXTRA_COLUMN"
        );
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When
        String sql = sqlBuilder.buildInsertSql(tableName, mappedColumns, false);

        // Then
        assertNotNull(sql);
        assertTrue(sql.startsWith("INSERT INTO SEND_TRANSACTIONS"));
        assertTrue(sql.contains("TRAN_ID"));
        assertTrue(sql.contains("TRAN_TYPE"));
        assertTrue(sql.contains("ACCT_NUM"));
        assertTrue(sql.contains("VALUES"));
        assertTrue(sql.contains(":TRAN_ID"));
        assertTrue(sql.contains(":TRAN_TYPE"));
        assertTrue(sql.contains(":ACCT_NUM"));
    }

    @Test
    @DisplayName("Should filter out columns that don't exist in DB")
    void testBuildInsertSqlFilterNonExistentColumns() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList(
                "TRAN_ID", "TRAN_TYPE", "NON_EXISTENT_COLUMN"
        ));

        // Mock DB columns (NON_EXISTENT_COLUMN not in DB)
        List<String> dbColumns = Arrays.asList("TRAN_ID", "TRAN_TYPE");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When
        String sql = sqlBuilder.buildInsertSql(tableName, mappedColumns, false);

        // Then
        assertNotNull(sql);
        assertTrue(sql.contains("TRAN_ID"));
        assertTrue(sql.contains("TRAN_TYPE"));
        assertFalse(sql.contains("NON_EXISTENT_COLUMN"));
    }

    @Test
    @DisplayName("Should add ID column when autoGenerateId is true")
    void testBuildInsertSqlWithAutoGenerateId() {
        // Given
        String tableName = "SEND_RECIP_DTL";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList("TRAN_ID", "SEND_FIRST_NAM"));

        // Mock DB columns including ID
        List<String> dbColumns = Arrays.asList("ID", "TRAN_ID", "SEND_FIRST_NAM");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_RECIP_DTL")))
                .thenReturn(dbColumns);

        // When
        String sql = sqlBuilder.buildInsertSql(tableName, mappedColumns, true);

        // Then
        assertNotNull(sql);
        assertTrue(sql.contains("ID"));
        assertTrue(sql.contains(":ID"));
        assertTrue(sql.contains("TRAN_ID"));
        assertTrue(sql.contains("SEND_FIRST_NAM"));
    }

    @Test
    @DisplayName("Should not duplicate ID column if already in mapped columns")
    void testBuildInsertSqlNoDuplicateId() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList("ID", "TRAN_ID"));

        // Mock DB columns
        List<String> dbColumns = Arrays.asList("ID", "TRAN_ID");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When
        String sql = sqlBuilder.buildInsertSql(tableName, mappedColumns, true);

        // Then
        assertNotNull(sql);
        // Count occurrences of "ID" in column list
        int idCount = (sql.split("\\bID\\b").length - 1);
        assertEquals(2, idCount); // Should appear exactly twice (column list and values list)
    }

    @Test
    @DisplayName("Should throw exception when no columns to insert")
    void testBuildInsertSqlNoColumns() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList("NON_EXISTENT"));

        // Mock DB columns (no matching columns)
        List<String> dbColumns = Arrays.asList("TRAN_ID", "TRAN_TYPE");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When & Then
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            sqlBuilder.buildInsertSql(tableName, mappedColumns, false);
        });

        assertTrue(exception.getMessage().contains("No columns to insert"));
    }

    @Test
    @DisplayName("Should cache database columns for performance")
    void testColumnCaching() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList("TRAN_ID"));

        List<String> dbColumns = Arrays.asList("TRAN_ID", "TRAN_TYPE");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When - call twice
        sqlBuilder.buildInsertSql(tableName, mappedColumns, false);
        sqlBuilder.buildInsertSql(tableName, mappedColumns, false);

        // Then - should only query DB once due to caching
        verify(jdbcTemplate, times(1)).queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS"));
    }

    @Test
    @DisplayName("Should clear cache when clearCache is called")
    void testClearCache() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList("TRAN_ID"));

        List<String> dbColumns = Arrays.asList("TRAN_ID");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When
        sqlBuilder.buildInsertSql(tableName, mappedColumns, false);
        sqlBuilder.clearCache();
        sqlBuilder.buildInsertSql(tableName, mappedColumns, false);

        // Then - should query DB twice (once before clear, once after)
        verify(jdbcTemplate, times(2)).queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS"));
    }

    @Test
    @DisplayName("Should handle empty mapped columns set")
    void testEmptyMappedColumns() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new HashSet<>();

        List<String> dbColumns = Arrays.asList("TRAN_ID", "TRAN_TYPE");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When & Then
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            sqlBuilder.buildInsertSql(tableName, mappedColumns, false);
        });

        assertTrue(exception.getMessage().contains("No columns to insert"));
    }

    @Test
    @DisplayName("Should use uppercase table name for Oracle metadata query")
    void testUppercaseTableName() {
        // Given
        String tableName = "send_transactions"; // lowercase
        Set<String> mappedColumns = new HashSet<>(Arrays.asList("TRAN_ID"));

        List<String> dbColumns = Arrays.asList("TRAN_ID");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When
        sqlBuilder.buildInsertSql(tableName, mappedColumns, false);

        // Then
        verify(jdbcTemplate).queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS"));
    }

    @Test
    @DisplayName("Should create valid SQL with multiple columns")
    void testMultipleColumnsSql() {
        // Given
        String tableName = "SEND_TRANSACTIONS";
        Set<String> mappedColumns = new LinkedHashSet<>(Arrays.asList(
                "TRAN_ID", "TRAN_TYPE", "ACCT_NUM", "NTWRK_CD", "CORLTN_ID"
        ));

        List<String> dbColumns = Arrays.asList(
                "TRAN_ID", "TRAN_TYPE", "ACCT_NUM", "NTWRK_CD", "CORLTN_ID"
        );
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRANSACTIONS")))
                .thenReturn(dbColumns);

        // When
        String sql = sqlBuilder.buildInsertSql(tableName, mappedColumns, false);

        // Then
        assertNotNull(sql);
        assertTrue(sql.matches("INSERT INTO SEND_TRANSACTIONS \\([^)]+\\) VALUES \\([^)]+\\)"));

        // Verify all columns are present
        for (String column : mappedColumns) {
            assertTrue(sql.contains(column));
            assertTrue(sql.contains(":" + column));
        }
    }

    @Test
    @DisplayName("Should handle special characters in column names")
    void testSpecialCharactersInColumnNames() {
        // Given
        String tableName = "SEND_TRAN_DTL";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList(
                "TRAN_ID", "ACQ_ICA", "ACQ_IDEN_CD"
        ));

        List<String> dbColumns = Arrays.asList("TRAN_ID", "ACQ_ICA", "ACQ_IDEN_CD");
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRAN_DTL")))
                .thenReturn(dbColumns);

        // When
        String sql = sqlBuilder.buildInsertSql(tableName, mappedColumns, false);

        // Then
        assertNotNull(sql);
        assertTrue(sql.contains("ACQ_ICA"));
        assertTrue(sql.contains("ACQ_IDEN_CD"));
        assertTrue(sql.contains(":ACQ_ICA"));
        assertTrue(sql.contains(":ACQ_IDEN_CD"));
    }

    @Test
    @DisplayName("Should handle address table with parent ID field")
    void testAddressTableWithParentId() {
        // Given
        String tableName = "SEND_TRAN_ADDR_DTL";
        Set<String> mappedColumns = new HashSet<>(Arrays.asList(
                "TRAN_ID", "ST_LINE1", "CITY", "ADDR_TYPE"
        ));

        List<String> dbColumns = Arrays.asList(
                "ID", "TRAN_ID", "ST_LINE1", "CITY", "ADDR_TYPE"
        );
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("SEND_TRAN_ADDR_DTL")))
                .thenReturn(dbColumns);

        // When
        String sql = sqlBuilder.buildInsertSql(tableName, mappedColumns, true);

        // Then
        assertNotNull(sql);
        assertTrue(sql.contains("ID"));
        assertTrue(sql.contains("TRAN_ID"));
        assertTrue(sql.contains("ADDR_TYPE"));
    }
}