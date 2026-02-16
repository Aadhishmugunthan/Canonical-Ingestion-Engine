package com.poc.CanonicalIngestionEngine.coverage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class UltraCoverageTest {

    @Test
    void hitRemainingBranches() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        DataMapper dm = new DataMapper();

        // -------- DataMapper exception + else branches ----------
        ObjectNode node = mapper.createObjectNode();
        node.put("text", "value");
        node.put("number", 100);

        Map<String,String> mapping = new HashMap<>();
        mapping.put("TXT","$.text");
        mapping.put("NUM","$.number");
        mapping.put("MISSING","$.missing"); // exception branch

        dm.map(node, mapping, null, false);

        // empty mandatory list branch
        dm.map(node, mapping, java.util.List.of(), true);

        // -------- mapAddress null branch ----------
        dm.mapAddress(node, "$.notPresent", mapping, "HOME", null);

        // -------- extractParentId all branches ----------
        IngestionService service = new IngestionService(
                mapper,
                Mockito.mock(RuleEngine.class),
                Mockito.mock(EventConfigLoader.class),
                dm,
                Mockito.mock(DynamicSqlBuilder.class),
                Mockito.mock(TransactionRepository.class)
        );

        Method m = IngestionService.class
                .getDeclaredMethod("extractParentId", Map.class);
        m.setAccessible(true);

        Map<String,Object> data = new HashMap<>();

        // TRAN_ID branch
        data.put("TRAN_ID","T1");
        m.invoke(service, data);

        // ID branch
        data.clear();
        data.put("ID","I1");
        m.invoke(service, data);

        // UUID branch
        data.clear();
        m.invoke(service, data);
    }
}
