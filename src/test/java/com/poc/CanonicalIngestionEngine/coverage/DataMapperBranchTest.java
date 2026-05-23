package com.poc.CanonicalIngestionEngine.coverage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataMapperBranchTest {

    private final DataMapper dm =
            new DataMapper();

    private final ObjectMapper mapper =
            new ObjectMapper();

    @Test
    void coverAllMapperBranches() throws Exception {

        String json = """
        {
          "name":"John",
          "empty":"",
          "num":10,
          "bool":true,
          "date":"2024-01-01T10:15:30Z"
        }
        """;

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put("NAME", "$.name");
        mapping.put("EMPTY", "$.empty");
        mapping.put("NUM", "$.num");
        mapping.put("BOOL", "$.bool");
        mapping.put("DATE_DT", "$.date");
        mapping.put("MISSING", "$.missing");

        Map<String, Object> result =
                dm.map(
                        mapper.readTree(json),
                        mapping,
                        Arrays.asList("NAME"),
                        true
                );

        assertEquals(
                "John",
                result.get("NAME")
        );

        assertNull(
                result.get("EMPTY")
        );

        assertEquals(
                10,
                result.get("NUM")
        );

        assertEquals(
                true,
                result.get("BOOL")
        );

        assertNotNull(
                result.get("DATE_DT")
        );

        assertNull(
                result.get("MISSING")
        );

        assertNotNull(
                result.get("ID")
        );

        Map<String, Object> address =
                dm.mapAddress(
                        mapper.readTree(json),
                        "$",
                        mapping,
                        "HOME",
                        "P1"
                );

        assertNotNull(address);

        assertEquals(
                "HOME",
                address.get("ADDR_TYPE")
        );

        assertEquals(
                "P1",
                address.get("PARENT_ID")
        );
    }
}