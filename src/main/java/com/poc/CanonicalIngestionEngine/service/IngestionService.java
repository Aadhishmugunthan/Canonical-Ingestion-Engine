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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final ObjectMapper objectMapper;
    private final RuleEngine ruleEngine;
    private final EventConfigLoader eventConfigLoader;
    private final DataMapper dataMapper;
    private final DynamicSqlBuilder sqlBuilder;
    private final TransactionRepository repository;

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

    // ================= MAIN ENTRY =================

    @Transactional
    public void ingest(EventEnvelope envelope) {

        try {

            log.info("Processing event | eventId={} | eventName={}",
                    envelope.getEventId(), envelope.getEventName());

            ruleEngine.apply(envelope);

            if (envelope.isIgnore()) {
                log.warn("Event ignored by rule engine | eventId={}", envelope.getEventId());
                return;
            }

            if (isUpdateOperation(envelope)) {
                updateFlow(envelope);
            } else {
                insertFlow(envelope);
            }

        } catch (Exception e) {
            throw new IngestionProcessingException(
                    "Failed to process eventId=" + envelope.getEventId(), e);
        }
    }

    // ================= CHECK UPDATE =================

    private boolean isUpdateOperation(EventEnvelope envelope) {

        try {
            JsonNode meta = objectMapper.readTree(envelope.getEventMetadata());
            return "U".equalsIgnoreCase(meta.path("operation").asText());
        } catch (Exception e) {
            return false;
        }
    }

    // ================= INSERT FLOW =================

    private void insertFlow(EventEnvelope envelope) {

        EventConfig config = getConfig(envelope);

        try {

            JsonNode payload = objectMapper.readTree(envelope.getEventPayload());
            String parentId = null;

            for (TableConfig table : config.getTables()) {

                if ("address".equalsIgnoreCase(table.getType())) {
                    processAddressInsert(payload, table, parentId);
                    continue;
                }

                parentId = processInsertTable(payload, table, parentId);
            }

        } catch (Exception e) {
            throw new IngestionProcessingException("Insert flow failed", e);
        }
    }

    private String processInsertTable(JsonNode payload, TableConfig table, String parentId) {

        Map<String, Object> data = dataMapper.map(
                payload, table.getMapping(), table.getMandatory(), table.isAutoGenerateId());

        if (data == null || data.isEmpty()) {
            return parentId;
        }

        // ================= ADD STATUS INIT =================
        if ("SEND_TRANSACTIONS".equalsIgnoreCase(table.getTableName())) {
            data.put("STATUS", "INIT");
        }
        // ===================================================

        String tranId = (String) data.get("TRAN_ID");

        if (tranId != null && repository.exists(table.getTableName(), "TRAN_ID", tranId)) {

            log.warn("Skipping insert. TRAN_ID exists in {}", table.getTableName());

            if ("main".equalsIgnoreCase(table.getType()) && parentId == null) {
                parentId = extractParentId(data);
            }

            return parentId;
        }

        String sql = sqlBuilder.buildInsertSql(
                table.getTableName(), data.keySet(), table.isAutoGenerateId());

        repository.insert(sql, data);

        log.info("Inserted into {}", table.getTableName());

        if ("main".equalsIgnoreCase(table.getType()) && parentId == null) {
            parentId = extractParentId(data);
        }

        return parentId;
    }

    private void processAddressInsert(JsonNode payload, TableConfig table, String parentId) {

        if (parentId == null || table.getAddressTypes() == null) {
            return;
        }

        table.getAddressTypes().forEach(addr -> {

            Map<String, Object> addressData = dataMapper.mapAddress(
                    payload,
                    addr.getRootPath(),
                    addr.getFields(),
                    addr.getType(),
                    parentId);

            if (addressData == null || addressData.isEmpty()) {
                return;
            }

            Object parentVal = addressData.remove("PARENT_ID");
            addressData.put(table.getParentIdField(), parentVal);

            String tranId = parentVal != null ? parentVal.toString() : null;

            boolean exists = tranId != null &&
                    repository.existsWithType(
                            table.getTableName(),
                            table.getParentIdField(),
                            tranId,
                            "ADDR_TYPE",
                            addr.getType());

            if (exists) {
                log.warn("Skipping address insert {}", addr.getType());
                return;
            }

            String sql = sqlBuilder.buildInsertSql(
                    table.getTableName(), addressData.keySet(), true);

            repository.insert(sql, addressData);

            log.info("Inserted address {}", addr.getType());
        });
    }

    // ================= UPDATE FLOW =================

    private void updateFlow(EventEnvelope envelope) {

        EventConfig config = getConfig(envelope);

        try {

            JsonNode payload = objectMapper.readTree(envelope.getEventPayload());
            String parentId = null;

            for (TableConfig table : config.getTables()) {

                if ("address".equalsIgnoreCase(table.getType())) {
                    log.warn("Skipping address update");
                    continue;
                }

                parentId = processUpdateTable(payload, table, parentId);
            }

        } catch (Exception e) {
            throw new IngestionProcessingException("Update flow failed", e);
        }
    }

    private String processUpdateTable(JsonNode payload, TableConfig table, String parentId) {

        Map<String, Object> data = dataMapper.map(
                payload, table.getMapping(), table.getMandatory(), table.isAutoGenerateId());

        if (data == null || data.isEmpty()) {
            return parentId;
        }

        // ================= ADD STATUS COMPLETED =================
        if ("SEND_TRANSACTIONS".equalsIgnoreCase(table.getTableName())) {
            data.put("STATUS", "COMPLETED");
        }
        // ========================================================

        String tranId = (String) data.get("TRAN_ID");

        if (tranId == null) {
            return parentId;
        }

        boolean exists = repository.exists(table.getTableName(), "TRAN_ID", tranId);

        if (exists) {

            String sql = sqlBuilder.buildUpdateSql(
                    table.getTableName(), data.keySet(), "TRAN_ID");

            repository.update(sql, data);

            log.info("Updated {}", table.getTableName());

        } else {

            String sql = sqlBuilder.buildInsertSql(
                    table.getTableName(), data.keySet(), table.isAutoGenerateId());

            repository.insert(sql, data);

            log.info("Inserted missing record {}", table.getTableName());
        }

        if ("main".equalsIgnoreCase(table.getType()) && parentId == null) {
            parentId = extractParentId(data);
        }

        return parentId;
    }

    private EventConfig getConfig(EventEnvelope envelope) {

        EventConfig config = eventConfigLoader.get(envelope.getEventName());

        if (config == null) {
            throw new IngestionProcessingException(
                    "No config found for event " + envelope.getEventName());
        }

        return config;
    }

    private String extractParentId(Map<String, Object> data) {

        Object id = data.get("TRAN_ID");

        if (id == null) {
            id = data.get("ID");
        }

        if (id == null) {
            id = UUID.randomUUID().toString();
        }

        return id.toString();
    }

    // ================= CUSTOM EXCEPTION =================

    public static class IngestionProcessingException extends RuntimeException {

        public IngestionProcessingException(String message) {
            super(message);
        }

        public IngestionProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}