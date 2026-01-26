package com.poc.CanonicalIngestionEngine.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class AddressBuilder {

    public List<Map<String,Object>> build(JsonNode root, AddressConfig cfg, String parentId) {
        List<Map<String,Object>> result = new ArrayList<>();

        if (cfg == null || cfg.getAddresses() == null) {
            return result;
        }

        for(AddressRule rule : cfg.getAddresses()) {
            String rootPath = rule.getRootPath();
            JsonNode addrNode;

            if(rootPath.startsWith("$")) {
                addrNode = resolveJsonPath(root, rootPath);
            } else {
                addrNode = root.at(rootPath);
            }

            if(addrNode == null || addrNode.isMissingNode() || addrNode.isNull()) {
                continue;
            }

            if(addrNode.isArray()) {
                for(JsonNode n : addrNode) {
                    Map<String,Object> addr = mapFields(n, rule);
                    addr.put("ID", UUID.randomUUID().toString());
                    addr.put("ADDR_TYPE", rule.getType());
                    result.add(addr);
                }
            } else {
                Map<String,Object> addr = mapFields(addrNode, rule);
                addr.put("ID", UUID.randomUUID().toString());
                addr.put("ADDR_TYPE", rule.getType());
                result.add(addr);
            }
        }
        return result;
    }

    private JsonNode resolveJsonPath(JsonNode root, String jsonPath) {
        if(jsonPath.equals("$")) {
            return root;
        }
        String pointer = jsonPath
                .replace("$.", "/")
                .replace(".", "/");
        return root.at(pointer.startsWith("/") ? pointer : "/" + pointer);
    }

    private Map<String,Object> mapFields(JsonNode node, AddressRule rule) {
        Map<String,Object> map = new HashMap<>();

        for(var e : rule.getFields().entrySet()) {
            String jsonPath = e.getValue();
            String column = e.getKey();
            JsonNode v = resolveJsonPath(node, jsonPath);

            if (v == null || v.isMissingNode() || v.isNull()) {
                map.put(column, null);
            } else {
                String text = v.asText();
                map.put(column, text.trim().isEmpty() ? null : text);
            }
        }
        return map;
    }
}