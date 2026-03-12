package com.poc.CanonicalIngestionEngine.repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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

@ExtendWith(MockitoExtension.class)
class TransactionRepositoryTest {

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    private TransactionRepository repository;

    @BeforeEach
    void setUp() {
        repository = new TransactionRepository(jdbcTemplate);
    }

    // ================= INSERT TESTS =================

    @Test
    @DisplayName("Should insert successfully")
    void testInsertSuccess() {
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID) VALUES (:TRAN_ID)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "PAY90002");

        // Fix: Removed useless eq() wrappers — pass values directly (SonarQube Low)
        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        assertDoesNotThrow(() -> repository.insert(sql, params));
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should skip insert on DuplicateKeyException")
    void testInsertDuplicateKey() {
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID) VALUES (:TRAN_ID)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "PAY90002");

        // Fix: Removed useless eq() wrappers — pass values directly (SonarQube Low)
        when(jdbcTemplate.update(sql, params))
                .thenThrow(new DuplicateKeyException("Duplicate key"));

        assertDoesNotThrow(() -> repository.insert(sql, params));
    }

    @Test
    @DisplayName("Should throw IllegalStateException on insert failure")
    void testInsertThrowsException() {
        String sql = "INSERT INTO SEND_TRANSACTIONS (TRAN_ID) VALUES (:TRAN_ID)";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "PAY90002");

        // Fix: Removed useless eq() wrappers — pass values directly (SonarQube Low)
        when(jdbcTemplate.update(sql, params))
                .thenThrow(new RuntimeException("DB error"));

        assertThrows(IllegalStateException.class,
                () -> repository.insert(sql, params));
    }

    // ================= UPDATE TESTS =================

    @Test
    @DisplayName("Should update successfully")
    void testUpdateSuccess() {
        String sql = "UPDATE SEND_TRANSACTIONS SET STATUS = :STATUS WHERE TRAN_ID = :TRAN_ID";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "PAY90002");
        params.put("STATUS", "COMPLETED");

        // Fix: Removed useless eq() wrappers — pass values directly (SonarQube Low)
        when(jdbcTemplate.update(sql, params)).thenReturn(1);

        assertDoesNotThrow(() -> repository.update(sql, params));
        verify(jdbcTemplate, times(1)).update(sql, params);
    }

    @Test
    @DisplayName("Should handle zero rows update")
    void testUpdateZeroRows() {
        String sql = "UPDATE SEND_TRANSACTIONS SET STATUS = :STATUS WHERE TRAN_ID = :TRAN_ID";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "PAY_NOT_FOUND");

        // Fix: Removed useless eq() wrappers — pass values directly (SonarQube Low)
        when(jdbcTemplate.update(sql, params)).thenReturn(0);

        assertDoesNotThrow(() -> repository.update(sql, params));
    }

    @Test
    @DisplayName("Should throw IllegalStateException on update failure")
    void testUpdateThrowsException() {
        String sql = "UPDATE SEND_TRANSACTIONS SET STATUS = :STATUS WHERE TRAN_ID = :TRAN_ID";
        Map<String, Object> params = new HashMap<>();
        params.put("TRAN_ID", "PAY90002");

        // Fix: Removed useless eq() wrappers — pass values directly (SonarQube Low)
        when(jdbcTemplate.update(sql, params))
                .thenThrow(new RuntimeException("DB error"));

        assertThrows(IllegalStateException.class,
                () -> repository.update(sql, params));
    }

    // ================= EXISTS TESTS =================

    @Test
    @DisplayName("Should return true when exists")
    void testExistsTrue() {
        // anyString() / anyMap() are fine — no eq() needed here (already correct)
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenReturn(1);

        boolean result = repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "PAY90002");
        assertTrue(result);
    }

    @Test
    @DisplayName("Should return false when not exists")
    void testExistsFalse() {
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenReturn(0);

        boolean result = repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "PAY00000");
        assertFalse(result);
    }

    @Test
    @DisplayName("Should throw exception when exists DB fails")
    void testExistsException() {
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenThrow(new RuntimeException("DB error"));

        assertThrows(IllegalStateException.class, () ->
                repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "PAY90002"));
    }

    @Test
    @DisplayName("Should return false when count null")
    void testExistsNull() {
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenReturn(null);

        boolean result = repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "PAY90002");
        assertFalse(result);
    }

    // ================= EXISTS WITH TYPE =================

    @Test
    @DisplayName("Should return true when existsWithType")
    void testExistsWithTypeTrue() {
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenReturn(1);

        boolean result = repository.existsWithType(
                "SEND_TRAN_ADDR_DTL", "TRAN_ID", "PAY90002", "ADDR_TYPE", "DEBTOR");

        assertTrue(result);
    }

    @Test
    @DisplayName("Should return false when existsWithType not found")
    void testExistsWithTypeFalse() {
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenReturn(0);

        boolean result = repository.existsWithType(
                "SEND_TRAN_ADDR_DTL", "TRAN_ID", "PAY90002", "ADDR_TYPE", "CREDITOR");

        assertFalse(result);
    }

    @Test
    @DisplayName("Should throw exception when existsWithType DB fails")
    void testExistsWithTypeException() {
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenThrow(new RuntimeException("DB error"));

        assertThrows(IllegalStateException.class, () ->
                repository.existsWithType(
                        "SEND_TRAN_ADDR_DTL",
                        "TRAN_ID",
                        "PAY90002",
                        "ADDR_TYPE",
                        "DEBTOR"));
    }

    @Test
    @DisplayName("Should return false when existsWithType count null")
    void testExistsWithTypeNull() {
        when(jdbcTemplate.queryForObject(anyString(), anyMap(), eq(Integer.class)))
                .thenReturn(null);

        boolean result = repository.existsWithType(
                "SEND_TRAN_ADDR_DTL", "TRAN_ID", "PAY90002", "ADDR_TYPE", "BANK");

        assertFalse(result);
    }
}