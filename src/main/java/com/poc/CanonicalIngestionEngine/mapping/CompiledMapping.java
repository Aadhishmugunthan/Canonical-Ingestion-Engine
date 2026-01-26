package com.poc.CanonicalIngestionEngine.mapping;

import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import org.mvel2.util.Make;

import java.util.List;
import java.util.Map;

@Data
public class CompiledMapping {
    private Map<String, JsonPath> paths;
    private List<String> mandatory;
}
