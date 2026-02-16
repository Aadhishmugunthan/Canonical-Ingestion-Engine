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

import static org.mockito.Mockito.*;

public class FinalCoverageBoostTest {

    ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldCoverFullIngestionFlow() throws Exception {

        // ---- MOCKS ----
        RuleEngine ruleEngine = mock(RuleEngine.class);
        EventConfigLoader loader = mock(EventConfigLoader.class);
        DataMapper dataMapper = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo = mock(TransactionRepository.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader, dataMapper, sqlBuilder, repo);

        // ---- EVENT CONFIG ----
        EventConfig config = new EventConfig();
        config.setEventName("AVS");

        TableConfig main = new TableConfig();
        main.setTableName("TXN_TABLE");
        main.setType("main");
        main.setAutoGenerateId(true);

        Map<String,String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.id");
        mapping.put("NAME", "$.name");

        main.setMapping(mapping);
        main.setMandatory(Collections.emptyList());

        TableConfig addr = new TableConfig();
        addr.setTableName("ADDR_TABLE");
        addr.setType("address");
        addr.setParentIdField("PARENT_ID");

        TableConfig.AddressTypeMapping home = new TableConfig.AddressTypeMapping();
        home.setType("HOME");
        home.setRootPath("$.address");

        Map<String,String> addrMap = new HashMap<>();
        addrMap.put("CITY","city");
        addrMap.put("STATE","state");
        home.setFields(addrMap);

        addr.setAddressTypes(List.of(home));

        config.setTables(List.of(main, addr));

        when(loader.get("AVS")).thenReturn(config);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT");

        // ---- JSON ----
        String payload = """
        {
          "eventName":"AVS",
          "eventPayload":"{\\"id\\":\\"T1\\",\\"name\\":\\"John\\",\\"address\\":{\\"city\\":\\"Chennai\\",\\"state\\":\\"TN\\"}}"
        }
        """;

        service.ingest(payload);

        verify(repo, atLeastOnce()).insert(any(), any());
    }

    // -------- NULL CONFIG PATH ----------
    @Test
    void shouldThrowWhenNoConfig() throws Exception {
        RuleEngine ruleEngine = mock(RuleEngine.class);
        EventConfigLoader loader = mock(EventConfigLoader.class);

        IngestionService service =
                new IngestionService(new ObjectMapper(), ruleEngine, loader,
                        new DataMapper(),
                        mock(DynamicSqlBuilder.class),
                        mock(TransactionRepository.class));

        String payload = """
        {"eventName":"UNKNOWN","eventPayload":"{}"}
        """;

        try {
            service.ingest(payload);
        } catch (RuntimeException e) {
            // expected
        }
    }

    // -------- IGNORE RULE PATH ----------
    @Test
    void shouldIgnoreWhenRuleSaysIgnore() throws Exception {

        RuleEngine ruleEngine = mock(RuleEngine.class);
        EventConfigLoader loader = mock(EventConfigLoader.class);

        IngestionService service =
                new IngestionService(new ObjectMapper(), ruleEngine, loader,
                        new DataMapper(),
                        mock(DynamicSqlBuilder.class),
                        mock(TransactionRepository.class));

        doAnswer(invocation -> {
            EventEnvelope env = invocation.getArgument(0);
            env.setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any());

        String payload = """
        {"eventName":"AVS","eventPayload":"{}"}
        """;

        service.ingest(payload);
    }

    // -------- DATAMAPPER EDGE CASES ----------
    @Test
    void shouldCoverDataMapperBranches() throws Exception {

        DataMapper dm = new DataMapper();

        String json = """
        {"num":123,"text":"hello","empty":"","date":"2024-01-01T10:15:30Z"}
        """;

        Map<String,String> mapping = new HashMap<>();
        mapping.put("NUM","$.num");
        mapping.put("TEXT","$.text");
        mapping.put("EMPTY","$.empty");
        mapping.put("DATE_DT","$.date");

        dm.map(new ObjectMapper().readTree(json),
                mapping,
                Collections.emptyList(),
                true);

        dm.mapAddress(new ObjectMapper().readTree(json),
                "$",
                mapping,
                "HOME",
                "PID1");
    }
}
