package com.poc.CanonicalIngestionEngine.repository;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

/**
 * 💾 Database repository for inserting transaction data
 *
 * Uses NamedParameterJdbcTemplate for named parameter binding (:TRAN_ID, :TRAN_TYPE, etc.)
 */
@Repository
public class TransactionRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public TransactionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 🔥 Insert data into database
     *
     * @param sql - INSERT SQL with named parameters (e.g., :TRAN_ID)
     * @param params - Map of parameter values (e.g., {TRAN_ID: "12345"})
     */
    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)

    public void insert(String sql, Map<String, Object> params) {

        try {
            jdbcTemplate.update(sql, params);

        } catch (DuplicateKeyException e) {
            // Duplicate key - ignore (idempotency)
            System.out.println("      ⚠️  Duplicate key detected - skipping insert");
        }
    }
}