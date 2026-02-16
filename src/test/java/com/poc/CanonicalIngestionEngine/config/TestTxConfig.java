package com.poc.CanonicalIngestionEngine.config;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;

@TestConfiguration
public class TestTxConfig {

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new AbstractPlatformTransactionManager() {
            @Override
            protected Object doGetTransaction() {
                return new Object();
            }

            @Override
            protected void doBegin(Object transaction, org.springframework.transaction.TransactionDefinition definition) {
                // NO-OP
            }

            @Override
            protected void doCommit(org.springframework.transaction.support.DefaultTransactionStatus status) {
                // NO-OP
            }

            @Override
            protected void doRollback(org.springframework.transaction.support.DefaultTransactionStatus status) {
                // NO-OP
            }
        };
    }
}
