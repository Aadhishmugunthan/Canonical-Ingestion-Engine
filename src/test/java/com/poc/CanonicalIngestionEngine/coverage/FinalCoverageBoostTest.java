package com.poc.CanonicalIngestionEngine.coverage;

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
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class FinalCoverageBoostTest {

    // ── shared helpers ──────────────────────────────────────────────────────

    /**
     * Builds a minimal EventEnvelope directly — no JSON string deserialization.
     * The service ingest(EventEnvelope) signature requires a pre-parsed envelope.
     */
    private EventEnvelope buildEnvelope(String eventName, String eventPayload, String operation) {
        EventEnvelope env = new EventEnvelope();
        env.setEventId("EVT-TEST-001");
        env.setEventName(eventName);
        env.setEventPayload(eventPayload);
        // metadata is still a JSON string — the service parses it internally via readTree()
        env.setEventMetadata("{\"operation\":\"" + operation + "\"}");
        return env;
    }

    private IngestionService buildService(RuleEngine ruleEngine,
                                          EventConfigLoader loader,
                                          DataMapper dataMapper,
                                          DynamicSqlBuilder sqlBuilder,
                                          TransactionRepository repo) {
        return new IngestionService(
                new ObjectMapper(), ruleEngine, loader, dataMapper, sqlBuilder, repo
        );
    }

    // ── test: full ingestion flow (insert path) ─────────────────────────────

    @Test
    void shouldCoverFullIngestionFlow() {

        RuleEngine ruleEngine        = mock(RuleEngine.class);
        EventConfigLoader loader     = mock(EventConfigLoader.class);
        DataMapper dataMapper        = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo   = mock(TransactionRepository.class);

        IngestionService service = buildService(ruleEngine, loader, dataMapper, sqlBuilder, repo);

        // ── event config ──
        TableConfig main = new TableConfig();
        main.setTableName("TXN_TABLE");
        main.setType("main");
        main.setAutoGenerateId(true);
        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.id");
        mapping.put("NAME",    "$.name");
        main.setMapping(mapping);
        main.setMandatory(Collections.emptyList());

        TableConfig addr = new TableConfig();
        addr.setTableName("ADDR_TABLE");
        addr.setType("address");
        addr.setParentIdField("PARENT_ID");
        TableConfig.AddressTypeMapping home = new TableConfig.AddressTypeMapping();
        home.setType("HOME");
        home.setRootPath("$.address");
        Map<String, String> addrFields = new HashMap<>();
        addrFields.put("CITY",  "city");
        addrFields.put("STATE", "state");
        home.setFields(addrFields);
        addr.setAddressTypes(List.of(home));

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        config.setTables(List.of(main, addr));

        when(loader.get("AVS")).thenReturn(config);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT INTO TXN_TABLE");

        // ── build envelope directly — NOT from a JSON string ──
        String payload = "{\"id\":\"T1\",\"name\":\"John\",\"address\":{\"city\":\"Chennai\",\"state\":\"TN\"}}";
        EventEnvelope envelope = buildEnvelope("AVS", payload, "A");

        service.ingest(envelope);

        verify(repo, atLeastOnce()).insert(any(), any());
    }

    // ── test: no config → exception ─────────────────────────────────────────

    @Test
    void shouldThrowWhenNoConfig() {

        RuleEngine ruleEngine    = mock(RuleEngine.class);
        EventConfigLoader loader = mock(EventConfigLoader.class);
        when(loader.get("UNKNOWN")).thenReturn(null);

        IngestionService service = buildService(
                ruleEngine, loader, new DataMapper(),
                mock(DynamicSqlBuilder.class), mock(TransactionRepository.class)
        );

        EventEnvelope envelope = buildEnvelope("UNKNOWN", "{}", "A");

        assertThrows(RuntimeException.class, () -> service.ingest(envelope));
    }

    // ── test: rule engine marks event as ignored ─────────────────────────────

    @Test
    void shouldIgnoreWhenRuleSaysIgnore() {

        RuleEngine ruleEngine      = mock(RuleEngine.class);
        EventConfigLoader loader   = mock(EventConfigLoader.class);
        TransactionRepository repo = mock(TransactionRepository.class);

        doAnswer(invocation -> {
            EventEnvelope env = invocation.getArgument(0);
            env.setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any());

        IngestionService service = buildService(
                ruleEngine, loader, new DataMapper(),
                mock(DynamicSqlBuilder.class), repo
        );

        EventEnvelope envelope = buildEnvelope("AVS", "{}", "A");

        service.ingest(envelope);

        verify(repo, never()).insert(any(), any());
        verify(repo, never()).update(any(), any());
    }

    // ── test: DataMapper field branches ──────────────────────────────────────

    @Test
    void shouldCoverDataMapperBranches() throws Exception {

        DataMapper dm = new DataMapper();
        ObjectMapper objectMapper = new ObjectMapper();

        String json = "{\"num\":123,\"text\":\"hello\",\"empty\":\"\",\"date\":\"2024-01-01T10:15:30Z\"}";

        Map<String, String> mapping = new HashMap<>();
        mapping.put("NUM",     "$.num");
        mapping.put("TEXT",    "$.text");
        mapping.put("EMPTY",   "$.empty");
        mapping.put("DATE_DT", "$.date");

        Map<String, Object> result = dm.map(
                objectMapper.readTree(json),
                mapping,
                Collections.emptyList(),
                true
        );

        assertNotNull(result);
        assertFalse(result.isEmpty());
        // empty string fields should be mapped to null
        assertNull(result.get("EMPTY"));
        // numeric field should be preserved
        assertEquals(123, result.get("NUM"));
        // date field ending in _DT should be converted to a Timestamp
        assertNotNull(result.get("DATE_DT"));

        Map<String, Object> addressResult = dm.mapAddress(
                objectMapper.readTree(json),
                "$",
                mapping,
                "HOME",
                "PID1"
        );

        assertNotNull(addressResult);
        assertFalse(addressResult.isEmpty());
        assertEquals("HOME",  addressResult.get("ADDR_TYPE"));
        assertEquals("PID1",  addressResult.get("PARENT_ID"));
    }

    // ── test: update flow ─────────────────────────────────────────────────────

    @Test
    void shouldCoverUpdateFlow() {

        RuleEngine ruleEngine        = mock(RuleEngine.class);
        EventConfigLoader loader     = mock(EventConfigLoader.class);
        DataMapper dataMapper        = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo   = mock(TransactionRepository.class);

        IngestionService service = buildService(ruleEngine, loader, dataMapper, sqlBuilder, repo);

        TableConfig main = new TableConfig();
        main.setTableName("TXN_TABLE");
        main.setType("main");
        main.setAutoGenerateId(false);
        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.id");
        main.setMapping(mapping);
        main.setMandatory(Collections.emptyList());

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        config.setTables(List.of(main));

        when(loader.get("AVS")).thenReturn(config);
        when(repo.exists("TXN_TABLE", "TRAN_ID", "T1")).thenReturn(true);
        when(sqlBuilder.buildUpdateSql(any(), any(), any()))
                .thenReturn("UPDATE TXN_TABLE SET STATUS = :STATUS WHERE TRAN_ID = :TRAN_ID");

        EventEnvelope envelope = buildEnvelope("AVS", "{\"id\":\"T1\"}", "U");

        service.ingest(envelope);

        verify(repo, atLeastOnce()).update(any(), any());
        verify(repo, never()).insert(any(), any());
    }
}
