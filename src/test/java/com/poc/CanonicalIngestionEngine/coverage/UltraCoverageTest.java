package com.poc.CanonicalIngestionEngine.coverage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class UltraCoverageTest {

    @Test
    void hitRemainingBranches() throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        DataMapper dm = new DataMapper();

        // --------------------------------------------------
        // DataMapper branches
        // --------------------------------------------------

        ObjectNode node = mapper.createObjectNode();
        node.put("text", "value");
        node.put("number", 100);

        Map<String, String> mapping = new HashMap<>();
        mapping.put("TXT", "$.text");
        mapping.put("NUM", "$.number");
        mapping.put("MISSING", "$.missing");

        Map<String, Object> result =
                dm.map(
                        node,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                "value",
                result.get("TXT")
        );

        assertEquals(
                100,
                result.get("NUM")
        );

        assertNull(
                result.get("MISSING")
        );

        Map<String, Object> generated =
                dm.map(
                        node,
                        mapping,
                        java.util.List.of(),
                        true
                );

        assertNotNull(
                generated.get("ID")
        );

        Map<String, Object> address =
                dm.mapAddress(
                        node,
                        "$.notPresent",
                        mapping,
                        "HOME",
                        null
                );

        assertNull(address);

        // --------------------------------------------------
        // extractParentId branches
        // --------------------------------------------------

        IngestionService service =
                new IngestionService(
                        mapper,
                        Mockito.mock(RuleEngine.class),
                        Mockito.mock(EventConfigLoader.class),
                        dm,
                        Mockito.mock(DynamicSqlBuilder.class),
                        Mockito.mock(TransactionRepository.class)
                );

        Method method =
                IngestionService.class.getDeclaredMethod(
                        "extractParentId",
                        Map.class
                );

        method.setAccessible(true);

        Map<String, Object> data =
                new HashMap<>();

        // TRAN_ID branch

        data.put("TRAN_ID", "T1");

        Object tranResult =
                method.invoke(service, data);

        assertEquals(
                "T1",
                tranResult
        );

        // ID branch

        data.clear();

        data.put("ID", "I1");

        Object idResult =
                method.invoke(service, data);

        assertNotNull(idResult);

        assertTrue(
                idResult instanceof String
        );

        // Empty map branch

        data.clear();

        Object uuidResult =
                method.invoke(service, data);

        assertNotNull(uuidResult);

        assertTrue(
                uuidResult instanceof String
        );
    }
}