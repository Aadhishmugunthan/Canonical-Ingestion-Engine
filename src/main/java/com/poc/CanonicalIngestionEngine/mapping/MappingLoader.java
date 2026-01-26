package com.poc.CanonicalIngestionEngine.mapping;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jayway.jsonpath.JsonPath;
import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Component
public class MappingLoader {

    private final Map<String, CompiledMapping> cache = new HashMap<>();

    @PostConstruct
    public void load() throws Exception {

        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        File dir = new ClassPathResource("mapping").getFile();

        for (File f : dir.listFiles()) {

            System.out.println("Loading mapping file: " + f.getName());

            // Skip address configs
            if (f.getName().endsWith("_ADDR.yml")) {
                System.out.println("Skipping address mapping file: " + f.getName());
                continue;
            }

            // Detect YAML root type first
            JsonNode root = om.readTree(f);

            if (root.isArray()) {
                System.out.println("Skipping array-based YAML: " + f.getName());
                continue;
            }

            MappingConfig cfg = om.treeToValue(root, MappingConfig.class);

            Map<String, JsonPath> compiled = new HashMap<>();
            cfg.getMapping().forEach((k, v) ->
                    compiled.put(k, JsonPath.compile(v)));

            CompiledMapping c = new CompiledMapping();
            c.setPaths(compiled);
            c.setMandatory(cfg.getMandatory());

            cache.put(cfg.getTxnType(), c);

            System.out.println("Loaded mapping for txnType = " + cfg.getTxnType());
        }
    }

    public CompiledMapping get(String txnType) {
        return cache.get(txnType);
    }
}
