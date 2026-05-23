package com.poc.CanonicalIngestionEngine.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.config.EventConfig;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.config.TableConfig;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IngestionIntegrationTest {

    @Mock private RuleEngine ruleEngine;
    @Mock private EventConfigLoader eventConfigLoader;
    @Mock private DataMapper dataMapper;
    @Mock private DynamicSqlBuilder sqlBuilder;
    @Mock private TransactionRepository repository;

    private IngestionService ingestionService;

    @BeforeEach
    void setUp() {

        ingestionService = new IngestionService(
                new ObjectMapper(),
                ruleEngine,
                eventConfigLoader,
                dataMapper,
                sqlBuilder,
                repository
        );
    }

    @Test
    void shouldProcessInsertFlow() {

        EventEnvelope env = new EventEnvelope();

        env.setEventId("evt-int-001");
        env.setEventName("AVS");
        env.setEventMetadata("{\"operation\":\"A\"}");

        env.setEventPayload("""
        {
          "avsTranId":"TXN-100",
          "transactionType":"VERIFY"
        }
        """);

        TableConfig table = new TableConfig();

        table.setTableName("SEND_TRANSACTIONS");
        table.setType("main");

        EventConfig config = new EventConfig();

        config.setEventName("AVS");
        config.setTables(List.of(table));

        when(eventConfigLoader.get("AVS"))
                .thenReturn(config);

        Map<String,Object> mapped = new HashMap<>();

        mapped.put("TRAN_ID", "TXN-100");
        mapped.put("TRAN_TYPE", "VERIFY");

        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(mapped);

        when(repository.exists(anyString(), anyString(), anyString()))
                .thenReturn(false);

        when(sqlBuilder.buildInsertSql(anyString(), any(), anyBoolean()))
                .thenReturn("INSERT SQL");

        assertDoesNotThrow(
                () -> ingestionService.ingest(env)
        );

        verify(repository)
                .insert(anyString(), any());
    }
}