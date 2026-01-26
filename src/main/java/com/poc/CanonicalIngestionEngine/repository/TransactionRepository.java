package com.poc.CanonicalIngestionEngine.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Map;

@Repository
public class TransactionRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public TransactionRepository(NamedParameterJdbcTemplate jdbc){
        this.jdbc=jdbc;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void insert(String sql, Map<String,Object> p){
        try{
            jdbc.update(sql,p);
        }catch(DuplicateKeyException e){
            return;
        }
    }
}

