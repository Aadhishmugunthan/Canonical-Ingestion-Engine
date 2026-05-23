package com.poc.CanonicalIngestionEngine.sql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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

    @BeforeEach
    void setUp() {

        sqlBuilder =
                new DynamicSqlBuilder(jdbcTemplate);
    }

    // =====================================================
    // HELPER
    // =====================================================

    private void mockDbColumns(
            String table,
            List<String> cols
    ) {

        when(jdbcTemplate.queryForList(
                anyString(),
                eq(String.class),
                eq(table)
        )).thenReturn(cols);
    }

    // =====================================================
    // INSERT TESTS
    // =====================================================

    @Test
    @DisplayName("INSERT - all columns exist")
    void testInsert_AllColumnsExist() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Arrays.asList(
                        "TRAN_ID",
                        "TRAN_TYPE",
                        "ACCT_NUM"
                )
        );

        String sql =
                sqlBuilder.buildInsertSql(
                        "SEND_TRANSACTIONS",
                        new LinkedHashSet<>(
                                Arrays.asList(
                                        "TRAN_ID",
                                        "TRAN_TYPE",
                                        "ACCT_NUM"
                                )
                        ),
                        false
                );

        assertNotNull(sql);

        assertTrue(
                sql.contains(
                        "INSERT INTO SEND_TRANSACTIONS"
                )
        );

        assertTrue(
                sql.contains("TRAN_ID")
        );

        assertTrue(
                sql.contains(":TRAN_ID")
        );
    }

    @Test
    @DisplayName("INSERT - filters invalid DB columns")
    void testInsert_FiltersInvalidColumns() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Arrays.asList(
                        "TRAN_ID",
                        "STATUS"
                )
        );

        String sql =
                sqlBuilder.buildInsertSql(
                        "SEND_TRANSACTIONS",
                        new LinkedHashSet<>(
                                Arrays.asList(
                                        "TRAN_ID",
                                        "STATUS",
                                        "BAD_COLUMN"
                                )
                        ),
                        false
                );

        assertFalse(
                sql.contains("BAD_COLUMN")
        );

        assertTrue(
                sql.contains("STATUS")
        );
    }

    @Test
    @DisplayName("INSERT - autoGenerateId adds ID")
    void testInsert_AutoGenerateId() {

        mockDbColumns(
                "SEND_RECIP_DTL",
                Arrays.asList(
                        "ID",
                        "TRAN_ID"
                )
        );

        String sql =
                sqlBuilder.buildInsertSql(
                        "SEND_RECIP_DTL",
                        new LinkedHashSet<>(
                                Collections.singletonList(
                                        "TRAN_ID"
                                )
                        ),
                        true
                );

        assertTrue(sql.contains("ID"));
        assertTrue(sql.contains(":ID"));
    }

    @Test
    @DisplayName("INSERT - throws when no valid columns")
    void testInsert_ThrowsWhenNoValidColumns() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Collections.singletonList("TRAN_ID")
        );

        Set<String> invalid =
                new HashSet<>(
                        Collections.singletonList(
                                "BAD"
                        )
                );

        IllegalStateException ex =
                assertThrows(
                        IllegalStateException.class,
                        () -> sqlBuilder.buildInsertSql(
                                "SEND_TRANSACTIONS",
                                invalid,
                                false
                        )
                );

        assertTrue(
                ex.getMessage()
                        .contains("No valid columns")
        );
    }

    @Test
    @DisplayName("INSERT - cache works")
    void testInsert_CacheWorks() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Collections.singletonList("TRAN_ID")
        );

        sqlBuilder.buildInsertSql(
                "SEND_TRANSACTIONS",
                new HashSet<>(
                        Collections.singletonList(
                                "TRAN_ID"
                        )
                ),
                false
        );

        sqlBuilder.buildInsertSql(
                "SEND_TRANSACTIONS",
                new HashSet<>(
                        Collections.singletonList(
                                "TRAN_ID"
                        )
                ),
                false
        );

        verify(
                jdbcTemplate,
                times(1)
        ).queryForList(
                anyString(),
                eq(String.class),
                eq("SEND_TRANSACTIONS")
        );
    }

    // =====================================================
    // UPDATE TESTS
    // =====================================================

    @Test
    @DisplayName("UPDATE - basic update SQL")
    void testUpdate_BasicSql() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Arrays.asList(
                        "TRAN_ID",
                        "STATUS"
                )
        );

        String sql =
                sqlBuilder.buildUpdateSql(
                        "SEND_TRANSACTIONS",
                        new LinkedHashSet<>(
                                Arrays.asList(
                                        "TRAN_ID",
                                        "STATUS"
                                )
                        ),
                        "TRAN_ID"
                );

        assertNotNull(sql);

        assertTrue(
                sql.contains(
                        "UPDATE SEND_TRANSACTIONS SET"
                )
        );

        assertTrue(
                sql.contains(
                        "STATUS = :STATUS"
                )
        );

        assertTrue(
                sql.contains(
                        "WHERE TRAN_ID = :TRAN_ID"
                )
        );
    }

    @Test
    @DisplayName("UPDATE - filters invalid columns")
    void testUpdate_FiltersInvalidColumns() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Arrays.asList(
                        "TRAN_ID",
                        "STATUS"
                )
        );

        String sql =
                sqlBuilder.buildUpdateSql(
                        "SEND_TRANSACTIONS",
                        new LinkedHashSet<>(
                                Arrays.asList(
                                        "TRAN_ID",
                                        "STATUS",
                                        "BAD"
                                )
                        ),
                        "TRAN_ID"
                );

        assertFalse(sql.contains("BAD"));

        assertTrue(
                sql.contains(
                        "STATUS = :STATUS"
                )
        );
    }

    @Test
    @DisplayName("UPDATE - whereColumn auto added")
    void testUpdate_WhereColumnAutoAdded() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Arrays.asList(
                        "TRAN_ID",
                        "STATUS"
                )
        );

        String sql =
                sqlBuilder.buildUpdateSql(
                        "SEND_TRANSACTIONS",
                        new LinkedHashSet<>(
                                Collections.singletonList(
                                        "STATUS"
                                )
                        ),
                        "TRAN_ID"
                );

        assertTrue(
                sql.contains(
                        "WHERE TRAN_ID = :TRAN_ID"
                )
        );
    }

    @Test
    @DisplayName("UPDATE - multiple columns creates commas")
    void testUpdate_MultipleColumnsCommaBranch() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Arrays.asList(
                        "TRAN_ID",
                        "STATUS",
                        "AMOUNT",
                        "TYPE"
                )
        );

        String sql =
                sqlBuilder.buildUpdateSql(
                        "SEND_TRANSACTIONS",
                        new LinkedHashSet<>(
                                Arrays.asList(
                                        "TRAN_ID",
                                        "STATUS",
                                        "AMOUNT",
                                        "TYPE"
                                )
                        ),
                        "TRAN_ID"
                );

        assertTrue(
                sql.contains(",")
        );

        assertTrue(
                sql.contains(
                        "STATUS = :STATUS"
                )
        );

        assertTrue(
                sql.contains(
                        "AMOUNT = :AMOUNT"
                )
        );

        assertTrue(
                sql.contains(
                        "TYPE = :TYPE"
                )
        );
    }

    @Test
    @DisplayName("UPDATE - cache reused")
    void testUpdate_UsesCache() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Arrays.asList(
                        "TRAN_ID",
                        "STATUS"
                )
        );

        sqlBuilder.buildInsertSql(
                "SEND_TRANSACTIONS",
                new LinkedHashSet<>(
                        Arrays.asList(
                                "TRAN_ID",
                                "STATUS"
                        )
                ),
                false
        );

        sqlBuilder.buildUpdateSql(
                "SEND_TRANSACTIONS",
                new LinkedHashSet<>(
                        Arrays.asList(
                                "TRAN_ID",
                                "STATUS"
                        )
                ),
                "TRAN_ID"
        );

        verify(
                jdbcTemplate,
                times(1)
        ).queryForList(
                anyString(),
                eq(String.class),
                eq("SEND_TRANSACTIONS")
        );
    }

    // =====================================================
    // CACHE CLEAR TEST
    // =====================================================

    @Test
    @DisplayName("CLEAR CACHE")
    void testClearCache() {

        mockDbColumns(
                "SEND_TRANSACTIONS",
                Collections.singletonList("TRAN_ID")
        );

        sqlBuilder.buildInsertSql(
                "SEND_TRANSACTIONS",
                new HashSet<>(
                        Collections.singletonList(
                                "TRAN_ID"
                        )
                ),
                false
        );

        sqlBuilder.clearCache();

        sqlBuilder.buildInsertSql(
                "SEND_TRANSACTIONS",
                new HashSet<>(
                        Collections.singletonList(
                                "TRAN_ID"
                        )
                ),
                false
        );

        verify(
                jdbcTemplate,
                times(2)
        ).queryForList(
                anyString(),
                eq(String.class),
                eq("SEND_TRANSACTIONS")
        );
    }
}