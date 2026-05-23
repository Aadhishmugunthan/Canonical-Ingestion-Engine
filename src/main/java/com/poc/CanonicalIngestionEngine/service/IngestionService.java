package com.poc.CanonicalIngestionEngine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.config.EventConfig;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.config.TableConfig;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import com.poc.CanonicalIngestionEngine.model.TransactionEventAxonMessage;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class IngestionService {

    private static final Logger log =
            LoggerFactory.getLogger(IngestionService.class);

    // =====================================================
    // STATUS TRANSITION RULES
    // Terminal statuses — once reached, cannot be overwritten
    // =====================================================
    private static final Set<String> TERMINAL_STATUSES = Set.of(
            "APPROVED",
            "SETTLED",
            "COMPLETED"
    );

    // Statuses that are considered "lower priority"
    // These should NOT overwrite terminal statuses
    private static final Set<String> LOW_PRIORITY_STATUSES = Set.of(
            "STARTED",
            "IN_PROGRESS",
            "PENDING",
            "INITIATED"
    );

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

    // =====================================================
    // MAIN ENTRY
    // =====================================================

    @Transactional
    public void ingest(EventEnvelope envelope) {

        try {

            log.info(
                    "Processing event | eventId={} | eventName={}",
                    envelope.getEventId(),
                    envelope.getEventName()
            );

            ruleEngine.apply(envelope);

            if (envelope.isIgnore()) {
                log.warn(
                        "Event ignored by rule engine | eventId={}",
                        envelope.getEventId()
                );
                return;
            }

            if (isUpdateOperation(envelope)) {
                updateFlow(envelope);
            } else {
                insertFlow(envelope);
            }

        } catch (Exception e) {
            throw new IngestionProcessingException(
                    "Failed to process eventId="
                            + envelope.getEventId(), e
            );
        }
    }

    // =====================================================
    // PARSE PAYLOAD
    // =====================================================

    private JsonNode parsePayload(String eventPayload) {
        try {
            // Validate DTO
            objectMapper.readValue(
                    eventPayload,
                    TransactionEventAxonMessage.class
            );
            // Return raw JsonNode for YAML mapping
            return objectMapper.readTree(eventPayload);

        } catch (Exception e) {
            throw new IngestionProcessingException(
                    "Failed to parse eventPayload", e
            );
        }
    }

    // =====================================================
    // STATUS TRANSITION VALIDATOR
    // =====================================================

    /**
     * Checks if the status transition is allowed.
     *
     * Rules:
     * 1. If DB status is TERMINAL (APPROVED, SETTLED, COMPLETED)
     *    AND incoming status is LOW PRIORITY (STARTED, IN_PROGRESS etc.)
     *    → BLOCK the update
     *
     * 2. If DB status is TERMINAL and incoming is also TERMINAL
     *    → ALLOW (e.g. INIT → SETTLED is fine)
     *
     * 3. If no existing record → ALLOW (fresh insert)
     */
    private boolean isStatusTransitionAllowed(
            String tranId,
            String incomingStatus
    ) {
        if (tranId == null || tranId.isBlank()) {
            return true; // New record — allow
        }

        if (incomingStatus == null || incomingStatus.isBlank()) {
            return true; // No incoming status — allow
        }

        try {
            // Check if record exists
            boolean exists = repository.exists(
                    "SEND_TRANSACTIONS", "TRAN_ID", tranId
            );

            if (!exists) {
                return true; // Fresh insert — allow
            }

            // Fetch current status from DB
            Map<String, Object> existingTxn =
                    repository.findTransaction(tranId);

            if (existingTxn == null || existingTxn.isEmpty()) {
                return true;
            }

            Object currentStatusObj = existingTxn.get("STATUS");

            if (currentStatusObj == null) {
                return true; // No current status — allow
            }

            String currentStatus =
                    currentStatusObj.toString().toUpperCase().trim();
            String incoming =
                    incomingStatus.toUpperCase().trim();

            // RULE: If DB status is TERMINAL and incoming is LOW PRIORITY
            // → BLOCK
            if (TERMINAL_STATUSES.contains(currentStatus) &&
                    LOW_PRIORITY_STATUSES.contains(incoming)) {

                log.warn(
                        "Status transition BLOCKED | TRAN_ID={} | " +
                                "currentStatus={} | incomingStatus={} | " +
                                "Reason: Cannot downgrade from terminal status",
                        tranId,
                        currentStatus,
                        incomingStatus
                );

                return false;
            }

            log.info(
                    "Status transition ALLOWED | TRAN_ID={} | " +
                            "currentStatus={} | incomingStatus={}",
                    tranId,
                    currentStatus,
                    incomingStatus
            );

            return true;

        } catch (Exception e) {
            log.warn(
                    "Could not validate status transition for TRAN_ID={} " +
                            "— allowing by default | error={}",
                    tranId,
                    e.getMessage()
            );
            return true; // If DB check fails, allow to avoid blocking
        }
    }

    // =====================================================
    // INSERT FLOW (PAYMENT / SETTLEMENT)
    // =====================================================

    private void insertFlow(EventEnvelope envelope) {

        EventConfig config = getConfig(envelope);

        try {

            JsonNode payload = parsePayload(
                    envelope.getEventPayload()
            );

            String parentId = null;

            for (TableConfig table : config.getTables()) {

                // Skip CLEARING table during insert flow
                if ("clearing".equalsIgnoreCase(table.getType()) ||
                        "child".equalsIgnoreCase(table.getType()) ||
                        "CLEARING".equalsIgnoreCase(
                                table.getTableName())) {

                    log.info(
                            "Skipping table '{}' during insert flow",
                            table.getTableName()
                    );
                    continue;
                }

                if ("address".equalsIgnoreCase(table.getType())) {
                    processAddressInsert(payload, table, parentId);
                    continue;
                }

                parentId = processInsertTable(
                        payload, table, parentId
                );
            }

        } catch (Exception e) {
            throw new IngestionProcessingException(
                    "Insert flow failed", e
            );
        }
    }

    private String processInsertTable(
            JsonNode payload,
            TableConfig table,
            String parentId
    ) {

        Map<String, Object> data =
                dataMapper.map(
                        payload,
                        table.getMapping(),
                        table.getMandatory(),
                        table.isAutoGenerateId()
                );

        if (data == null || data.isEmpty()) {
            return parentId;
        }

        applyDefaults(data);
        validateAndConvertTypes(data);

        if ("SEND_TRANSACTIONS".equalsIgnoreCase(
                table.getTableName())) {
            data.put("STATUS", "INIT");
        }

        String tranId = (String) data.get("TRAN_ID");

        if (tranId != null &&
                repository.exists(
                        table.getTableName(),
                        "TRAN_ID",
                        tranId
                )) {

            log.info(
                    "TRAN_ID already exists. Starting merge for table={}",
                    table.getTableName()
            );

            mergeNullFields(
                    table.getTableName(),
                    tranId,
                    data
            );

            if ("main".equalsIgnoreCase(table.getType())
                    && parentId == null) {

                parentId = extractParentId(data);
            }

            return parentId;
        }

        String sql = sqlBuilder.buildInsertSql(
                table.getTableName(),
                data.keySet(),
                table.isAutoGenerateId()
        );

        repository.insert(sql, data);

        log.info("Inserted into {}", table.getTableName());

        if ("main".equalsIgnoreCase(table.getType())
                && parentId == null) {
            parentId = extractParentId(data);
        }

        return parentId;
    }

    // =====================================================
    // MERGE NULL FIELDS
    // =====================================================

    private void mergeNullFields(
            String tableName,
            String tranId,
            Map<String, Object> incomingData
    ) {

        try {

            Map<String, Object> existingRow =
                    repository.findTransaction(tranId);

            if (existingRow == null ||
                    existingRow.isEmpty()) {

                log.warn(
                        "No existing row found for TRAN_ID={}",
                        tranId
                );

                return;
            }

            for (Map.Entry<String, Object> entry :
                    incomingData.entrySet()) {

                String column = entry.getKey();

                // ============================================
                // SKIP UNKNOWN DB COLUMNS
                // ============================================

                if (!repository.columnExists(
                        tableName,
                        column
                )) {

                    log.warn(
                            "Skipping unknown column={} table={}",
                            column,
                            tableName
                    );

                    continue;
                }

                Object incomingValue =
                        entry.getValue();

                Object existingValue =
                        existingRow.get(column);

                // ============================================
                // UPDATE ONLY IF:
                // EXISTING VALUE IS NULL
                // INCOMING VALUE IS NOT NULL
                // ============================================

                if ((existingValue == null ||
                        existingValue.toString().isBlank())
                        &&
                        incomingValue != null &&
                        !incomingValue.toString().isBlank()) {

                    repository.updateColumn(
                            tableName,
                            "TRAN_ID",
                            tranId,
                            column,
                            incomingValue
                    );

                    log.info(
                            "Merged column={} value={} for TRAN_ID={}",
                            column,
                            incomingValue,
                            tranId
                    );
                }
            }

        } catch (Exception e) {

            throw new IngestionProcessingException(
                    "Merge failed for TRAN_ID=" + tranId,
                    e
            );
        }
    }

    // =====================================================
    // UPDATE FLOW (CLEARING + AIS2 ENRICHMENT)
    // =====================================================

    private void updateFlow(EventEnvelope envelope) {

        EventConfig config = getConfig(envelope);

        try {

            JsonNode payload = parsePayload(
                    envelope.getEventPayload()
            );

            String tranId = null;

            String[] possibleTransactionFields = {
                    "accountInformationId",
                    "paymentTransactionId",
                    "transactionId",
                    "authorizationId",
                    "fundingTransactionId",
                    "avsTranId",
                    "nvsTranId",
                    "refundTransactionId",
                    "reversalTransactionId",
                    "settlementTransactionId"
            };

            for (String field : possibleTransactionFields) {

                if (payload.has(field)
                        && !payload.get(field).isNull()
                        && !payload.get(field).asText().isBlank()) {

                    tranId = payload.get(field).asText();

                    log.info(
                            "Transaction ID resolved using field={} value={}",
                            field,
                            tranId
                    );

                    break;
                }
            }

            if (tranId == null) {

                throw new RuntimeException(
                        "Transaction ID not found in payload"
                );
            }

            log.info(
                    "Processing UPDATE FLOW for TRAN_ID={}",
                    tranId
            );

            // =====================================================
            // STEP 1 — CHECK TRANSACTION EXISTS
            // =====================================================

            boolean exists = repository.exists(
                    "SEND_TRANSACTIONS",
                    "TRAN_ID",
                    tranId
            );

            if (!exists) {

                throw new RuntimeException(
                        "Transaction not found: " + tranId
                );
            }

            // =====================================================
            // AIS2 ENRICHMENT FLOW
            // =====================================================

            if ("AIS2".equalsIgnoreCase(
                    envelope.getEventName())) {

                log.info(
                        "Starting AIS2 enrichment for TRAN_ID={}",
                        tranId
                );

                Map<String, Object> existingTxn =
                        repository.findTransaction(tranId);

                if (existingTxn == null ||
                        existingTxn.isEmpty()) {

                    throw new RuntimeException(
                            "Existing transaction not found for AIS2 enrichment"
                    );
                }

                // =====================================================
                // FETCH EXISTING DB VALUE
                // =====================================================

                Object existingSwSerNum =
                        existingTxn.get("SW_SER_NUM");

                // =====================================================
                // FETCH INCOMING AIS2 VALUE
                // =====================================================

                String incomingSwSerNum =
                        payload.path("switchSerialNumber")
                                .asText(null);

                log.info(
                        "AIS2 enrichment | existing SW_SER_NUM={} | incoming SW_SER_NUM={}",
                        existingSwSerNum,
                        incomingSwSerNum
                );

                // =====================================================
                // UPDATE ONLY IF DB VALUE IS NULL
                // =====================================================

                if ((existingSwSerNum == null ||
                        existingSwSerNum.toString().isBlank())
                        &&
                        incomingSwSerNum != null &&
                        !incomingSwSerNum.isBlank()) {

                    repository.updateColumn(
                            "SEND_TRANSACTIONS",
                            "TRAN_ID",
                            tranId,
                            "SW_SER_NUM",
                            incomingSwSerNum
                    );

                    log.info(
                            "AIS2 enrichment SUCCESS | Updated SW_SER_NUM={} for TRAN_ID={}",
                            incomingSwSerNum,
                            tranId
                    );

                } else {

                    log.info(
                            "AIS2 enrichment SKIPPED | Existing SW_SER_NUM already populated for TRAN_ID={}",
                            tranId
                    );
                }

                return;
            }

            // =====================================================
            // NORMAL CLEARING FLOW
            // =====================================================

            log.info(
                    "Processing CLEARING flow for TRAN_ID={}",
                    tranId
            );

            // STEP 2 — STATUS TRANSITION CHECK

            String incomingStatus =
                    payload.path("status").asText(null);

            if (!isStatusTransitionAllowed(
                    tranId,
                    incomingStatus
            )) {

                log.warn(
                        "Clearing update SKIPPED due to invalid " +
                                "status transition | TRAN_ID={} | " +
                                "incomingStatus={}",
                        tranId,
                        incomingStatus
                );

                return;
            }

            // STEP 3 — PULL ALL RELATED DATA

            Map<String, List<Map<String, Object>>> relatedData =
                    repository.findAllRelatedData(tranId);

            log.info(
                    "Fetched related data for TRAN_ID={} | " +
                            "SEND_TRANSACTIONS={} | SEND_TRAN_DTL={} | " +
                            "SEND_RECIP_DTL={} | SEND_TRAN_ADDR_DTL={}",
                    tranId,
                    relatedData.getOrDefault(
                            "SEND_TRANSACTIONS", List.of()).size(),
                    relatedData.getOrDefault(
                            "SEND_TRAN_DTL", List.of()).size(),
                    relatedData.getOrDefault(
                            "SEND_RECIP_DTL", List.of()).size(),
                    relatedData.getOrDefault(
                            "SEND_TRAN_ADDR_DTL", List.of()).size()
            );

            // =====================================================
            // STEP 4 — FIND UPDATE TABLE CONFIG DYNAMICALLY
            // =====================================================

            TableConfig updateTable = config.getTables()
                    .stream()
                    .filter(t ->
                            !"address".equalsIgnoreCase(t.getType())
                                    &&
                                    !"child".equalsIgnoreCase(t.getType())
                    )
                    .findFirst()
                    .orElseThrow(() ->
                            new IngestionProcessingException(
                                    envelope.getEventName()
                                            + " update table config not found"
                            )
                    );

            // =====================================================
            // STEP 5 — BUILD UPDATE DATA
            // =====================================================

            Map<String, Object> updateData =
                    new HashMap<>();

            updateData.put("TRAN_ID", tranId);

            Map<String, Object> mappedData =
                    dataMapper.map(
                            payload,
                            updateTable.getMapping(),
                            List.of(),
                            updateTable.isAutoGenerateId()
                    );

            if (mappedData != null) {

                mappedData.remove("TRAN_ID");

                updateData.putAll(mappedData);
            }

            log.info(
                    "Update data built: {} fields for TRAN_ID={}",
                    updateData.size(),
                    tranId
            );

            // =====================================================
            // STEP 6 — MERGE NULL FIELDS DYNAMICALLY
            // =====================================================

            mergeNullFields(
                    updateTable.getTableName(),
                    tranId,
                    updateData
            );

            log.info(
                    "Dynamic merge completed for TRAN_ID={} table={}",
                    tranId,
                    updateTable.getTableName()
            );

            // =====================================================
            // STEP 7 — OPTIONAL STATUS UPDATE
            // =====================================================

            incomingStatus =
                    payload.path("status").asText(null);

            if (incomingStatus != null &&
                    !incomingStatus.isBlank()) {

                repository.updateStatus(
                        tranId,
                        incomingStatus
                );

                log.info(
                        "Transaction status updated={} for TRAN_ID={}",
                        incomingStatus,
                        tranId
                );
            }

        } catch (IngestionProcessingException e) {

            throw e;

        } catch (Exception e) {

            throw new IngestionProcessingException(
                    "Update flow failed",
                    e
            );
        }
    }

    // =====================================================
    // DEFAULT HANDLER
    // =====================================================

    private void applyDefaults(Map<String, Object> data) {

        if (data.get("CUR_STAT") == null) {
            data.put("CUR_STAT", "00");
        }

        if (data.get("NON_FIN_TXN") == null) {
            data.put("NON_FIN_TXN", 0);
        }

        if (data.get("RPLCTN_UPDT_TS") == null) {
            data.put("RPLCTN_UPDT_TS", data.get("TRAN_CRTE_DT"));
        }
    }

    // =====================================================
    // TYPE VALIDATION
    // =====================================================

    private void validateAndConvertTypes(
            Map<String, Object> data
    ) {

        if (data.get("NON_FIN_TXN") != null) {
            try {
                data.put(
                        "NON_FIN_TXN",
                        Integer.parseInt(
                                data.get("NON_FIN_TXN").toString()
                        )
                );
            } catch (NumberFormatException e) {
                log.warn(
                        "NON_FIN_TXN value '{}' is not numeric, defaulting to 0",
                        data.get("NON_FIN_TXN")
                );
                data.put("NON_FIN_TXN", 0);
            }
        }

        if (data.get("TRAN_AMT") != null) {
            try {
                Double amt = Double.parseDouble(
                        data.get("TRAN_AMT").toString()
                );
                if (amt < 0) {
                    throw new RuntimeException(
                            "TRAN_AMT cannot be negative"
                    );
                }
                data.put("TRAN_AMT", amt);
            } catch (NumberFormatException e) {
                log.warn(
                        "TRAN_AMT value '{}' is not numeric, skipping",
                        data.get("TRAN_AMT")
                );
                data.remove("TRAN_AMT");
            }
        }
    }

    // =====================================================
    // ADDRESS INSERT
    // =====================================================

    private void processAddressInsert(
            JsonNode payload,
            TableConfig table,
            String parentId
    ) {

        if (parentId == null ||
                table.getAddressTypes() == null) {
            return;
        }

        table.getAddressTypes().forEach(addr -> {

            Map<String, Object> data =
                    dataMapper.mapAddress(
                            payload,
                            addr.getRootPath(),
                            addr.getFields(),
                            addr.getType(),
                            parentId
                    );

            if (data == null || data.isEmpty()) {
                return;
            }

            Object parentVal = data.remove("PARENT_ID");
            data.put(table.getParentIdField(), parentVal);

            String sql = sqlBuilder.buildInsertSql(
                    table.getTableName(),
                    data.keySet(),
                    true
            );

            repository.insert(sql, data);

            log.info("Inserted address {}", addr.getType());
        });
    }

    // =====================================================
    // HELPERS
    // =====================================================

    private boolean isUpdateOperation(EventEnvelope envelope) {
        try {
            JsonNode meta =
                    objectMapper.readTree(
                            envelope.getEventMetadata()
                    );
            return "U".equalsIgnoreCase(
                    meta.path("operation").asText()
            );
        } catch (Exception e) {
            return false;
        }
    }

    public EventConfig getConfig(EventEnvelope envelope){
        EventConfig config =
                eventConfigLoader.get(envelope.getEventName());
        if (config == null) {
            throw new IngestionProcessingException(
                    "No config found for event: "
                            + envelope.getEventName()
            );
        }
        return config;
    }

    private String extractParentId(Map<String, Object> data) {
        Object id = data.get("TRAN_ID");
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
        return id.toString();
    }

    // =====================================================
    // CUSTOM EXCEPTION
    // =====================================================

    public static class IngestionProcessingException
            extends RuntimeException {

        public IngestionProcessingException(String message) {
            super(message);
        }

        public IngestionProcessingException(
                String message, Throwable cause
        ) {
            super(message, cause);
        }
    }
}