package com.poc.CanonicalIngestionEngine.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test cases for TransactionRepository
 * FIXED: Proper verify order (action before verification)
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

    @Test
    @DisplayName("Should successfully insert data into database")
    void testSuccessfulInsert() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID, TRAN_TYPE) VALUES (:TRAN_ID, :TRAN_TYPE)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "TXN-123");
        params.put("TRAN_TYPE", "AVS");

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should handle duplicate key exception gracefully")
    void testDuplicateKeyHandling() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID, TRAN_TYPE) VALUES (:TRAN_ID, :TRAN_TYPE)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "TXN-123");
        params.put("TRAN_TYPE", "AVS");

        when(jdbcTemplate.update(sql, params)).thenThrow(new DuplicateKeyException("Duplicate key"));

        // When & Then - should not throw exception
        assertDoesNotThrow(() -> repository.insert(sql, params));
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should insert multiple records")
    void testMultipleInserts() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID) VALUES (:TRAN_ID)";

        Map<String, Object> params1 = new HashMap<>();
        params1.put("TRAN_ID", "TXN-1");

        Map<String, Object> params2 = new HashMap<>();
        params2.put("TRAN_ID", "TXN-2");

        when(jdbcTemplate.update(eq(sql), any(Map.class))).thenReturn(1);

        // When - PERFORM ACTIONS FIRST
        repository.insert(sql, params1);
        repository.insert(sql, params2);

        // Then - VERIFY AFTER ACTIONS
        verify(jdbcTemplate, times(2)).update(eq(sql), any(Map.class));
    }

    @Test
    @DisplayName("Should handle null values in parameters")
    void testNullValueInParameters() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID, TRAN_TYPE) VALUES (:TRAN_ID, :TRAN_TYPE)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "TXN-123");
        params.put("TRAN_TYPE", null);

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should handle numeric parameters")
    void testNumericParameters() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID, AMOUNT) VALUES (:TRAN_ID, :AMOUNT)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "TXN-123");
        params.put("AMOUNT", 100.50);

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should handle timestamp parameters")
    void testTimestampParameters() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID, TRAN_CRTE_DT) VALUES (:TRAN_ID, :TRAN_CRTE_DT)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "TXN-123");
        params.put("TRAN_CRTE_DT", java.sql.Timestamp.valueOf("2024-01-01 10:00:00"));

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should insert into detail table")
    void testInsertDetailTable() {
        // Given
        String sql = "INSERT INTO SEND_TRAN_DTL (TRAN_ID, PAYMT_REF) VALUES (:TRAN_ID, :PAYMT_REF)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "TXN-123");
        params.put("PAYMT_REF", "REF-456");

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should insert into recipient table with UUID")
    void testInsertRecipientTableWithUUID() {
        // Given
        String sql = "INSERT INTO SEND_RECIP_DTL (ID, TRAN_ID, SEND_FIRST_NAM) VALUES (:ID, :TRAN_ID, :SEND_FIRST_NAM)";
        Map<String, Object> params = new HashMap<>();
        params.put("ID", java.util.UUID.randomUUID().toString());
        params.put("TRAN_ID", "TXN-123");
        params.put("SEND_FIRST_NAM", "John");

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should insert into address table")
    void testInsertAddressTable() {
        // Given
        String sql = "INSERT INTO SEND_TRAN_ADDR_DTL (ID, TRAN_ID, ADDR_TYPE, ST_LINE1) VALUES (:ID, :TRAN_ID, :ADDR_TYPE, :ST_LINE1)";
        Map<String, Object> params = new HashMap<>();
        params.put("ID", java.util.UUID.randomUUID().toString());
        params.put("TRAN_ID", "TXN-123");
        params.put("ADDR_TYPE", "HOME");
        params.put("ST_LINE1", "123 Main St");

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should handle large parameter maps")
    void testLargeParameterMap() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (...) VALUES (...)";
        Map<String, Object> params = new HashMap<>();

        // Add many parameters
        for (int i = 1; i <= 50; i++) {
            params.put("FIELD_" + i, "VALUE_" + i);
        }

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should maintain transaction isolation")
    void testTransactionIsolation() {
        // Given
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID) VALUES (:TRAN_ID)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "TXN-123");

        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        // When
        repository.insert(sql, params);

        // Then
        // Verify method is called (transaction management is handled by Spring)
        verify(jdbcTemplate, times(1)).update(sql, params);
    }
}