package com.poc.CanonicalIngestionEngine.coverage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import org.junit.jupiter.api.Test;

import java.util.*;

public class DataMapperBranchTest {

    DataMapper dm = new DataMapper();
    ObjectMapper mapper = new ObjectMapper();

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

        Map<String,String> mapping = new HashMap<>();
        mapping.put("NAME","$.name");
        mapping.put("EMPTY","$.empty");
        mapping.put("NUM","$.num");
        mapping.put("BOOL","$.bool");
        mapping.put("DATE_DT","$.date");
        mapping.put("MISSING","$.missing");

        dm.map(
                mapper.readTree(json),
                mapping,
                Arrays.asList("NAME"),
                true
        );

        dm.mapAddress(
                mapper.readTree(json),
                "$",
                mapping,
                "HOME",
                "P1"
        );
    }
}
