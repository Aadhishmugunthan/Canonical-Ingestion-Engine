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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

public class IngestionServiceBranchTest {

    // ── shared helper ───────────────────────────────────────────────────────

    /**
     * Constructs an EventEnvelope directly.
     * The service now accepts EventEnvelope — JSON string deserialization
     * belongs to the controller/listener layer, not the service.
     */
    private EventEnvelope buildEnvelope(String eventName, String eventPayload, String operation) {
        EventEnvelope env = new EventEnvelope();
        env.setEventId("EVT-BRANCH-001");
        env.setEventName(eventName);
        env.setEventPayload(eventPayload);
        // eventMetadata is still parsed internally by the service via objectMapper.readTree()
        env.setEventMetadata("{\"operation\":\"" + operation + "\"}");
        return env;
    }

    // ── test: main table insert branch ──────────────────────────────────────

    @Test
    void coverAllBranches() {

        ObjectMapper mapper          = new ObjectMapper();
        RuleEngine ruleEngine        = mock(RuleEngine.class);
        EventConfigLoader loader     = mock(EventConfigLoader.class);
        DataMapper dataMapper        = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo   = mock(TransactionRepository.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader, dataMapper, sqlBuilder, repo);

        // ── config ──
        TableConfig table = new TableConfig();
        table.setTableName("TEST");
        table.setType("main");
        table.setAutoGenerateId(true);
        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.id");
        table.setMapping(mapping);
        table.setMandatory(Collections.emptyList());

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        config.setTables(List.of(table));

        when(loader.get("AVS")).thenReturn(config);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT INTO TEST");

        // ── pass EventEnvelope directly, not a JSON string ──
        EventEnvelope envelope = buildEnvelope("AVS", "{\"id\":\"T1\"}", "A");

        service.ingest(envelope);

        verify(repo, atLeastOnce()).insert(any(), any());
    }

    // ── test: ignore branch ─────────────────────────────────────────────────

    @Test
    void coverIgnoreBranch() {

        RuleEngine ruleEngine      = mock(RuleEngine.class);
        EventConfigLoader loader   = mock(EventConfigLoader.class);
        TransactionRepository repo = mock(TransactionRepository.class);

        IngestionService service = new IngestionService(
                new ObjectMapper(), ruleEngine, loader,
                new DataMapper(),
                mock(DynamicSqlBuilder.class),
                repo
        );

        doAnswer(i -> {
            EventEnvelope e = i.getArgument(0);
            e.setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any());

        EventEnvelope envelope = buildEnvelope("AVS", "{}", "A");

        assertDoesNotThrow(() -> service.ingest(envelope));
        verify(repo, never()).insert(any(), any());
        verify(repo, never()).update(any(), any());
    }

    // ── test: update branch — existing row ──────────────────────────────────

    @Test
    void coverUpdateBranchExistingRow() {

        ObjectMapper mapper          = new ObjectMapper();
        RuleEngine ruleEngine        = mock(RuleEngine.class);
        EventConfigLoader loader     = mock(EventConfigLoader.class);
        DataMapper dataMapper        = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo   = mock(TransactionRepository.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader, dataMapper, sqlBuilder, repo);

        TableConfig table = new TableConfig();
        table.setTableName("TEST");
        table.setType("main");
        table.setAutoGenerateId(false);
        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.id");
        table.setMapping(mapping);
        table.setMandatory(Collections.emptyList());

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        config.setTables(List.of(table));

        when(loader.get("AVS")).thenReturn(config);
        when(repo.exists("TEST", "TRAN_ID", "T1")).thenReturn(true);
        when(sqlBuilder.buildUpdateSql(any(), any(), any()))
                .thenReturn("UPDATE TEST SET STATUS = :STATUS WHERE TRAN_ID = :TRAN_ID");

        EventEnvelope envelope = buildEnvelope("AVS", "{\"id\":\"T1\"}", "U");

        assertDoesNotThrow(() -> service.ingest(envelope));
        verify(repo, atLeastOnce()).update(any(), any());
        verify(repo, never()).insert(any(), any());
    }

    // ── test: update branch — row not found, falls back to insert ───────────

    @Test
    void coverUpdateBranchRowNotFound() {

        ObjectMapper mapper          = new ObjectMapper();
        RuleEngine ruleEngine        = mock(RuleEngine.class);
        EventConfigLoader loader     = mock(EventConfigLoader.class);
        DataMapper dataMapper        = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo   = mock(TransactionRepository.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader, dataMapper, sqlBuilder, repo);

        TableConfig table = new TableConfig();
        table.setTableName("TEST");
        table.setType("main");
        table.setAutoGenerateId(false);
        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.id");
        table.setMapping(mapping);
        table.setMandatory(Collections.emptyList());

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        config.setTables(List.of(table));

        when(loader.get("AVS")).thenReturn(config);
        when(repo.exists("TEST", "TRAN_ID", "T_NEW")).thenReturn(false);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT INTO TEST");

        EventEnvelope envelope = buildEnvelope("AVS", "{\"id\":\"T_NEW\"}", "U");

        assertDoesNotThrow(() -> service.ingest(envelope));
        verify(repo, atLeastOnce()).insert(any(), any());
        verify(repo, never()).update(any(), any());
    }

    // ── test: empty mapped data is skipped silently ──────────────────────────

    @Test
    void coverEmptyMappedDataBranch() {

        ObjectMapper mapper          = new ObjectMapper();
        RuleEngine ruleEngine        = mock(RuleEngine.class);
        EventConfigLoader loader     = mock(EventConfigLoader.class);
        DataMapper dataMapper        = mock(DataMapper.class);
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo   = mock(TransactionRepository.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader, dataMapper, sqlBuilder, repo);

        TableConfig table = new TableConfig();
        table.setTableName("TEST");
        table.setType("main");
        table.setAutoGenerateId(false);
        table.setMapping(new HashMap<>());
        table.setMandatory(Collections.emptyList());

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        config.setTables(List.of(table));

        when(loader.get("AVS")).thenReturn(config);
        // Return empty map → service should skip insert silently
        when(dataMapper.map(any(), any(), any(), anyBoolean())).thenReturn(new HashMap<>());

        EventEnvelope envelope = buildEnvelope("AVS", "{}", "A");

        assertDoesNotThrow(() -> service.ingest(envelope));
        verify(repo, never()).insert(any(), any());
        verify(repo, never()).update(any(), any());
    }

    // ── test: address table skipped when parentId is null ───────────────────

    @Test
    void coverAddressSkippedWhenParentIdNull() {

        ObjectMapper mapper          = new ObjectMapper();
        RuleEngine ruleEngine        = mock(RuleEngine.class);
        EventConfigLoader loader     = mock(EventConfigLoader.class);
        DataMapper dataMapper        = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo   = mock(TransactionRepository.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader, dataMapper, sqlBuilder, repo);

        TableConfig.AddressTypeMapping addrType = new TableConfig.AddressTypeMapping();
        addrType.setType("HOME");
        addrType.setRootPath("$.address");
        addrType.setFields(new HashMap<>());

        TableConfig addr = new TableConfig();
        addr.setTableName("ADDR_TABLE");
        addr.setType("address");
        addr.setParentIdField("PARENT_ID");
        addr.setAddressTypes(List.of(addrType));

        EventConfig config = new EventConfig();
        config.setEventName("AVS");
        // Only address table, no main table → parentId stays null → address skipped
        config.setTables(List.of(addr));

        when(loader.get("AVS")).thenReturn(config);

        EventEnvelope envelope = buildEnvelope("AVS", "{\"address\":{\"city\":\"Chennai\"}}", "A");

        assertDoesNotThrow(() -> service.ingest(envelope));
        verify(repo, never()).insert(any(), any());
    }
}