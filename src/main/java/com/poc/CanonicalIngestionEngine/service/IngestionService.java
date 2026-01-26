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
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
        if(env.isIgnore()) {
            System.out.println("IGNORED by rules");
            System.out.println("========== INGESTION END ==========\n");
            return;
        }

        EventConfig eventConfig = eventConfigLoader.get(env.getEventName());
        if (eventConfig == null) {
            throw new RuntimeException("No config for: " + env.getEventName());
        }

        System.out.println("Config: " + eventConfig.getTables().size() + " tables");

        JsonNode payload = mapper.readTree(env.getEventPayload());

        String parentId = null;
        int count = 0;

        for (TableConfig table : eventConfig.getTables()) {
            count++;
            System.out.println("\n[" + count + "/" + eventConfig.getTables().size() + "] " +
                    table.getMappingKey() + " (" + table.getType() + ")");

            if ("address".equals(table.getType())) {
                processAddressTable(payload, table, parentId);
            } else {
                parentId = processRegularTable(payload, table, parentId);
            }
        }

        System.out.println("\n✓ SUCCESS");
        System.out.println("========== INGESTION END ==========\n");
    }

    private String processRegularTable(JsonNode payload, TableConfig table, String currentParentId) {
        CompiledMapping mapping = mappingLoader.get(table.getMappingKey());
        if (mapping == null) {
            System.err.println("  ✗ No mapping for " + table.getMappingKey());
            return currentParentId;
        }

        System.out.println("  Columns: " + mapping.getPaths().size());

        Map<String,Object> data = cm.map(payload, mapping);
        convertDatesToTimestamp(data);

        String sqlStatement = sql.get(table.getSqlKey());
        if (sqlStatement == null) {
            System.err.println("  ✗ No SQL for " + table.getSqlKey());
            return currentParentId;
        }

        repo.insert(sqlStatement, data);
        System.out.println("  ✓ Inserted");

        if (currentParentId == null && "main".equals(table.getType())) {
            currentParentId = extractParentId(data);
            System.out.println("  Parent ID: " + currentParentId);
        }

        return currentParentId;
    }

    private void processAddressTable(JsonNode payload, TableConfig table, String parentId) {
        if (parentId == null) {
            System.err.println("  ✗ No parent ID");
            return;
        }

        System.out.println("  Parent: " + parentId + " (field: " + table.getParentIdField() + ")");

        String eventName = table.getMappingKey().replace("_ADDR", "");

        AddressConfig addrCfg = addrLoader.get(eventName);
        if (addrCfg == null) {
            System.err.println("  ✗ No address config for " + eventName);
            return;
        }

        List<Map<String,Object>> addresses = addrBuilder.build(payload, addrCfg, parentId);
        System.out.println("  Addresses: " + addresses.size());

        int i = 0;
        for(Map<String,Object> addr : addresses) {
            i++;
            addr.put(table.getParentIdField(), parentId);
            convertDatesToTimestamp(addr);

            String sqlStatement = sql.get(table.getSqlKey());
            if (sqlStatement == null) {
                System.err.println("  ✗ No SQL for " + table.getSqlKey());
                continue;
            }

            repo.insert(sqlStatement, addr);
            System.out.println("  ✓ [" + i + "] " + addr.get("ADDR_TYPE"));
        }
    }

    private String extractParentId(Map<String,Object> data) {
        String[] idFields = {"TRAN_ID", "ACCT_INFO_ID", "TRANSACTION_ID", "ID"};
        for (String field : idFields) {
            Object value = data.get(field);
            if (value != null) {
                return value.toString();
            }
        }
        return UUID.randomUUID().toString();
    }

    private void convertDatesToTimestamp(Map<String,Object> data) {
        for (Map.Entry<String,Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if ((key.endsWith("_DT") || key.endsWith("_TS") ||
                    key.contains("_CRTE_") || key.contains("DATE") || key.contains("_LOC_DT"))
                    && value instanceof String) {
                try {
                    String dateStr = (String) value;
                    Instant instant;

                    if (dateStr.contains("T")) {
                        instant = Instant.parse(dateStr.endsWith("Z") ? dateStr : dateStr + "Z");
                    } else {
                        instant = Instant.parse(dateStr);
                    }

                    data.put(key, Timestamp.from(instant));
                    System.out.println("  Date: " + key + " → " + dateStr);
                } catch (Exception e) {
                    System.err.println("  Date error: " + key + " = " + value);
                }
            }
        }
    }
}