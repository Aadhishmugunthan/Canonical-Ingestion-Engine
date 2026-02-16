package com.poc.CanonicalIngestionEngine.coverage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.config.*;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.mockito.Mockito.*;

public class IngestionServiceBranchTest {

    @Test
    void coverAllBranches() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        RuleEngine ruleEngine = mock(RuleEngine.class);
        EventConfigLoader loader = mock(EventConfigLoader.class);
        DataMapper dataMapper = new DataMapper();
        DynamicSqlBuilder sqlBuilder = mock(DynamicSqlBuilder.class);
        TransactionRepository repo = mock(TransactionRepository.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader, dataMapper, sqlBuilder, repo);

        EventConfig config = new EventConfig();
        config.setEventName("AVS");

        TableConfig table = new TableConfig();
        table.setTableName("TEST");
        table.setType("main");
        table.setAutoGenerateId(true);

        Map<String,String> mapping = new HashMap<>();
        mapping.put("TRAN_ID","$.id");
        table.setMapping(mapping);
        table.setMandatory(Collections.emptyList());

        config.setTables(List.of(table));

        when(loader.get("AVS")).thenReturn(config);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT");

        String json = """
        {"eventName":"AVS","eventPayload":"{\\"id\\":\\"T1\\"}"}
        """;

        service.ingest(json);

        verify(repo, atLeastOnce()).insert(any(), any());
    }

    @Test
    void coverIgnoreBranch() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        RuleEngine ruleEngine = mock(RuleEngine.class);
        EventConfigLoader loader = mock(EventConfigLoader.class);

        IngestionService service =
                new IngestionService(mapper, ruleEngine, loader,
                        new DataMapper(),
                        mock(DynamicSqlBuilder.class),
                        mock(TransactionRepository.class));

        doAnswer(i -> {
            com.poc.CanonicalIngestionEngine.model.EventEnvelope e = i.getArgument(0);
            e.setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any());

        String json = """
        {"eventName":"AVS","eventPayload":"{}"}
        """;

        service.ingest(json);
    }
}
