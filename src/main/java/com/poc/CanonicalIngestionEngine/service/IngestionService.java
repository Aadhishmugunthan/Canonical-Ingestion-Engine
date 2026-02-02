package com.poc.CanonicalIngestionEngine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.config.EventConfig;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.config.TableConfig;
import com.poc.CanonicalIngestionEngine.mapping.*;
import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.sql.SqlCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

@Service
public class IngestionService {

    @Autowired ObjectMapper mapper;
    @Autowired RuleEngine rules;
    @Autowired MappingLoader mappingLoader;
    @Autowired AddressConfigLoader addrLoader;
    @Autowired CanonicalMapper cm;
    @Autowired TransactionRepository repo;
    @Autowired AddressBuilder addrBuilder;
    @Autowired SqlCache sql;
    @Autowired EventConfigLoader eventConfigLoader;

    @Transactional
    public void ingest(String json) throws Exception {

        System.out.println("\n========== INGESTION START ==========");

        EventEnvelope env = mapper.readValue(json, EventEnvelope.class);

        System.out.println("Event: " + env.getEventName() + " (ID: " + env.getEventId() + ")");

        rules.apply(env);
        if (env.isIgnore()) {
            System.out.println("IGNORED by rules");
            return;
        }

        EventConfig eventConfig = eventConfigLoader.get(env.getEventName());
        if (eventConfig == null) {
            throw new RuntimeException("No config for: " + env.getEventName());
        }

        JsonNode payload = mapper.readTree(env.getEventPayload());

        String parentId = null;
        int count = 0;

        for (TableConfig table : eventConfig.getTables()) {

            count++;

            System.out.println("\n[" + count + "/" + eventConfig.getTables().size() + "] "
                    + table.getMappingKey() + " (" + table.getType() + ")");

            if ("address".equals(table.getType())) {
                processAddressTable(payload, table, parentId);
            } else {
                parentId = processRegularTable(payload, table, parentId);
            }
        }

        System.out.println("\n✅ SUCCESS");
        System.out.println("========== INGESTION END ==========\n");
    }

    private String processRegularTable(JsonNode payload,
                                       TableConfig table,
                                       String currentParentId) {

        CompiledMapping mapping = mappingLoader.get(table.getMappingKey());
        if (mapping == null) {
            System.err.println("✗ No mapping for " + table.getMappingKey());
            return currentParentId;
        }

        Map<String, Object> data = cm.map(payload, mapping);
        convertDatesToTimestamp(data);

        // ✅ Add unique ID for recipient rows
        if (table.getMappingKey().contains("RECIP")
                || table.getMappingKey().contains("SENDER")
                || table.getMappingKey().contains("RECEIVER")) {

            data.put("ID", UUID.randomUUID().toString());
            System.out.println("  Added Recipient ID: " + data.get("ID"));
        }

        String sqlStatement = sql.get(table.getSqlKey());
        if (sqlStatement == null) {
            System.err.println("✗ No SQL for " + table.getSqlKey());
            return currentParentId;
        }

        repo.insert(sqlStatement, data);
        System.out.println("✓ Inserted");

        if (currentParentId == null && "main".equals(table.getType())) {
            currentParentId = extractParentId(data);
            System.out.println("Parent ID: " + currentParentId);
        }

        return currentParentId;
    }

    private void processAddressTable(JsonNode payload,
                                     TableConfig table,
                                     String parentId) {

        if (parentId == null) return;

        String eventName = table.getMappingKey().replace("_ADDR", "");

        AddressConfig cfg = addrLoader.get(eventName);
        if (cfg == null) return;

        List<Map<String, Object>> addresses =
                addrBuilder.build(payload, cfg, parentId);

        for (Map<String, Object> addr : addresses) {

            addr.put(table.getParentIdField(), parentId);
            addr.put("ID", UUID.randomUUID().toString());

            repo.insert(sql.get(table.getSqlKey()), addr);
        }
    }

    private String extractParentId(Map<String, Object> data) {
        Object id = data.get("TRAN_ID");
        return id != null ? id.toString() : UUID.randomUUID().toString();
    }

    private void convertDatesToTimestamp(Map<String, Object> data) {

        for (String key : data.keySet()) {

            Object value = data.get(key);

            if (value instanceof String str &&
                    (key.endsWith("_DT") || key.endsWith("_TS"))) {

                try {
                    Instant instant = Instant.parse(str);
                    data.put(key, Timestamp.from(instant));
                } catch (Exception ignored) {}
            }
        }
    }
}
