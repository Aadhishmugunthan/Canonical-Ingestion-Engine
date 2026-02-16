package com.poc.CanonicalIngestionEngine.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.config.EventConfig;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.config.TableConfig;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@Tag("integration")
@ExtendWith(MockitoExtension.class)
class IngestionIntegrationTest {

    @Mock
    private RuleEngine ruleEngine;

    @Mock
    private EventConfigLoader eventConfigLoader;

    @Mock
    private DataMapper dataMapper;

    @Mock
    private DynamicSqlBuilder sqlBuilder;

    @Mock
    private TransactionRepository repository;

    private IngestionService ingestionService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        ingestionService = new IngestionService(
                objectMapper,
                ruleEngine,
                eventConfigLoader,
                dataMapper,
                sqlBuilder,
                repository
        );
    }

    @Test
    void shouldProcessAVSEndToEnd() throws Exception {
        // Given
        String json = """
        {
          "eventName":"AVS",
          "eventId":"evt-integration-001",
          "eventSource":"AVS_SERVICE",
          "correlationId":"corr-001",
          "eventTimestamp":1738656000000,
          "regulatoryRegion":"NA",
          "eventMetadata":"{\\"operation\\":\\"I\\"}",
          "eventPayload":"{\\"avsTranId\\":\\"TXN-100\\",\\"transactionType\\":\\"VERIFY\\",\\"acctNum\\":\\"123456\\"}"
        }
        """;

        EventConfig config = createEventConfig();
        Map<String, Object> mappedData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mappedData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());
        doNothing().when(repository).insert(anyString(), anyMap());

        // When
        assertDoesNotThrow(() -> ingestionService.ingest(json));

        // Then
        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    @Test
    void shouldIgnoreUpdateOperation() throws Exception {
        // Given
        String json = """
        {
          "eventName":"AVS",
          "eventId":"evt-update-001",
          "eventSource":"AVS_SERVICE",
          "correlationId":"corr-002",
          "eventTimestamp":1738656000000,
          "regulatoryRegion":"NA",
          "eventMetadata":"{\\"operation\\":\\"U\\"}",
          "eventPayload":"{\\"avsTranId\\":\\"TXN-200\\"}"
        }
        """;

        doAnswer(invocation -> {
            Object envelope = invocation.getArgument(0);
            envelope.getClass().getMethod("setIgnore", boolean.class).invoke(envelope, true);
            return null;
        }).when(ruleEngine).apply(any());

        // When
        assertDoesNotThrow(() -> ingestionService.ingest(json));

        // Then - No repository calls for UPDATE operations
        verify(repository, never()).insert(anyString(), anyMap());
    }

    @Test
    void shouldHandleInvalidEventGracefully() {
        // Given
        String invalidJson = """
        {
          "eventName":"UNKNOWN",
          "eventId":"evt-invalid-001",
          "eventMetadata":"{\\"operation\\":\\"I\\"}",
          "eventPayload":"{}"
        }
        """;

        when(eventConfigLoader.get("UNKNOWN")).thenReturn(null);
        doNothing().when(ruleEngine).apply(any());

        // When & Then
        assertThrows(RuntimeException.class, () -> ingestionService.ingest(invalidJson));
    }

    // Helper methods
    private EventConfig createEventConfig() {
        EventConfig config = new EventConfig();
        config.setEventName("AVS");

        TableConfig table = new TableConfig();
        table.setTableName("SEND_TRANSACTIONS");
        table.setType("main");
        table.setOrder(1);

        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("TRAN_TYPE", "$.transactionType");
        table.setMapping(mapping);

        table.setMandatory(Arrays.asList("TRAN_ID", "TRAN_TYPE"));
        config.setTables(Arrays.asList(table));

        return config;
    }

    private Map<String, Object> createMappedData() {
        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_ID", "TXN-100");
        data.put("TRAN_TYPE", "VERIFY");
        return data;
    }
}