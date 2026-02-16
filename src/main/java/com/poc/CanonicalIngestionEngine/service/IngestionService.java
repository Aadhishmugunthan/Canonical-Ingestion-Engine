package com.poc.CanonicalIngestionEngine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.config.EventConfig;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.config.TableConfig;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class IngestionService {

    private final ObjectMapper objectMapper;
    private final RuleEngine ruleEngine;
    private final EventConfigLoader eventConfigLoader;
    private final DataMapper dataMapper;
    private final DynamicSqlBuilder sqlBuilder;
    private final TransactionRepository repository;

    // 🔥 CONSTRUCTOR INJECTION (CRITICAL FIX)
    public IngestionService(
            ObjectMapper objectMapper,
            RuleEngine ruleEngine,
            EventConfigLoader eventConfigLoader,
            DataMapper dataMapper,
            DynamicSqlBuilder sqlBuilder,
            TransactionRepository repository
    ) {
        this.objectMapper = objectMapper;
        this.ruleEngine = ruleEngine;
        this.eventConfigLoader = eventConfigLoader;
        this.dataMapper = dataMapper;
        this.sqlBuilder = sqlBuilder;
        this.repository = repository;
    }

    @Transactional
    public void ingest(String jsonString) throws Exception {

        EventEnvelope envelope = objectMapper.readValue(jsonString, EventEnvelope.class);

        ruleEngine.apply(envelope);

        if (envelope.isIgnore()) {
            return;
        }

        EventConfig eventConfig = eventConfigLoader.get(envelope.getEventName());

        if (eventConfig == null) {
            throw new RuntimeException("No configuration found for event: " + envelope.getEventName());
        }

        JsonNode payload = objectMapper.readTree(envelope.getEventPayload());

        String parentId = null;

        for (TableConfig table : eventConfig.getTables()) {

            if ("address".equalsIgnoreCase(table.getType())) {
                processAddressTable(payload, table, parentId);
            } else {
                parentId = processRegularTable(payload, table, parentId);
            }
        }
    }

    private String processRegularTable(JsonNode payload,
                                       TableConfig table,
                                       String currentParentId) {

        Map<String, Object> data = dataMapper.map(
                payload,
                table.getMapping(),
                table.getMandatory(),
                table.isAutoGenerateId()
        );

        if (data == null || data.isEmpty()) {
            return currentParentId;
        }

        if (table.getParentIdField() != null && currentParentId != null) {
            data.put(table.getParentIdField(), currentParentId);
        }

        String sql = sqlBuilder.buildInsertSql(
                table.getTableName(),
                data.keySet(),
                table.isAutoGenerateId()
        );

        repository.insert(sql, data);

        if ("main".equalsIgnoreCase(table.getType()) && currentParentId == null) {
            currentParentId = extractParentId(data);
        }

        return currentParentId;
    }

    private void processAddressTable(JsonNode payload,
                                     TableConfig table,
                                     String parentId) {

        if (parentId == null) return;
        if (table.getAddressTypes() == null) return;

        for (TableConfig.AddressTypeMapping addressType : table.getAddressTypes()) {

            Map<String, Object> addressData = dataMapper.mapAddress(
                    payload,
                    addressType.getRootPath(),
                    addressType.getFields(),
                    addressType.getType(),
                    parentId
            );

            if (addressData == null || addressData.isEmpty()) continue;

            Object parentIdValue = addressData.remove("PARENT_ID");
            addressData.put(table.getParentIdField(), parentIdValue);

            String sql = sqlBuilder.buildInsertSql(
                    table.getTableName(),
                    addressData.keySet(),
                    true
            );

            repository.insert(sql, addressData);
        }
    }

    private String extractParentId(Map<String, Object> data) {

        Object id = data.get("TRAN_ID");
        if (id == null) id = data.get("ID");
        if (id == null) id = UUID.randomUUID().toString();

        return id.toString();
    }
}
