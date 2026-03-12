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

    // ── helper: build a fully populated EventEnvelope ──────────────────────
    //
    // The service ingest(EventEnvelope) signature requires a pre-parsed
    // envelope — JSON deserialization is the controller/listener's job.
    // eventMetadata and eventPayload remain JSON strings because the
    // service still calls objectMapper.readTree() on them internally.

    private EventEnvelope buildEnvelope(String eventId,
                                        String eventName,
                                        String eventSource,
                                        String correlationId,
                                        String regulatoryRegion,
                                        String metadataJson,
                                        String payloadJson) {
        EventEnvelope env = new EventEnvelope();
        env.setEventId(eventId);
        env.setEventName(eventName);
        env.setEventSource(eventSource);
        env.setCorrelationId(correlationId);
        env.setEventTimestamp(1738656000000L);
        env.setRegulatoryRegion(regulatoryRegion);
        env.setEventMetadata(metadataJson);
        env.setEventPayload(payloadJson);
        return env;
    }

    // ── test: full AVS insert end-to-end ───────────────────────────────────

    @Test
    void shouldProcessAVSEndToEnd() {

        EventEnvelope envelope = buildEnvelope(
                "evt-integration-001",
                "AVS",
                "AVS_SERVICE",
                "corr-001",
                "NA",
                "{\"operation\":\"I\"}",
                "{\"avsTranId\":\"TXN-100\",\"transactionType\":\"VERIFY\",\"acctNum\":\"123456\"}"
        );

        EventConfig config   = createEventConfig();
        Map<String, Object> mappedData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean())).thenReturn(mappedData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean())).thenReturn("INSERT SQL");
        doNothing().when(repository).insert(anyString(), anyMap());

        assertDoesNotThrow(() -> ingestionService.ingest(envelope));

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    // ── test: event marked ignored by rule engine ───────────────────────────

    @Test
    void shouldIgnoreWhenRuleEngineIgnoresEvent() {

        EventEnvelope envelope = buildEnvelope(
                "evt-update-001",
                "AVS",
                "AVS_SERVICE",
                "corr-002",
                "NA",
                "{\"operation\":\"U\"}",
                "{\"avsTranId\":\"TXN-200\"}"
        );

        // Rule engine sets ignore = true directly on the envelope
        doAnswer(invocation -> {
            ((EventEnvelope) invocation.getArgument(0)).setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any(EventEnvelope.class));

        assertDoesNotThrow(() -> ingestionService.ingest(envelope));

        verify(repository, never()).insert(anyString(), anyMap());
        verify(repository, never()).update(anyString(), anyMap());
    }

    // ── test: no config found → exception ──────────────────────────────────

    @Test
    void shouldHandleInvalidEventGracefully() {

        EventEnvelope envelope = buildEnvelope(
                "evt-invalid-001",
                "UNKNOWN",
                null,
                null,
                null,
                "{\"operation\":\"I\"}",
                "{}"
        );

        when(eventConfigLoader.get("UNKNOWN")).thenReturn(null);
        doNothing().when(ruleEngine).apply(any());

        assertThrows(RuntimeException.class, () -> ingestionService.ingest(envelope));
    }

    // ── test: update flow routes correctly ─────────────────────────────────

    @Test
    void shouldProcessUpdateFlowEndToEnd() {

        EventEnvelope envelope = buildEnvelope(
                "evt-update-002",
                "AVS",
                "AVS_SERVICE",
                "corr-003",
                "NA",
                "{\"operation\":\"U\"}",
                "{\"avsTranId\":\"TXN-300\",\"transactionType\":\"VERIFY\"}"
        );

        EventConfig config   = createEventConfig();
        Map<String, Object> mappedData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean())).thenReturn(mappedData);
        when(repository.exists("SEND_TRANSACTIONS", "TRAN_ID", "TXN-100")).thenReturn(true);
        when(sqlBuilder.buildUpdateSql(anyString(), anySet(), anyString())).thenReturn("UPDATE SQL");
        doNothing().when(repository).update(anyString(), anyMap());

        assertDoesNotThrow(() -> ingestionService.ingest(envelope));

        verify(repository, atLeastOnce()).update(anyString(), anyMap());
        verify(repository, never()).insert(anyString(), anyMap());
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private EventConfig createEventConfig() {
        TableConfig table = new TableConfig();
        table.setTableName("SEND_TRANSACTIONS");
        table.setType("main");
        table.setOrder(1);
        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID",    "$.avsTranId");
        mapping.put("TRAN_TYPE",  "$.transactionType");
        table.setMapping(mapping);
        table.setMandatory(Arrays.asList("TRAN_ID", "TRAN_TYPE"));

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        config.setTables(Arrays.asList(table));
        return config;
    }

    private Map<String, Object> createMappedData() {
        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_ID",   "TXN-100");
        data.put("TRAN_TYPE", "VERIFY");
        return data;
    }
}