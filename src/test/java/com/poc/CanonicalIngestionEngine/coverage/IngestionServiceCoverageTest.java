package com.poc.CanonicalIngestionEngine.coverage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.config.EventConfig;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.config.TableConfig;
import com.poc.CanonicalIngestionEngine.mapping.DataMapper;
import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import com.poc.CanonicalIngestionEngine.repository.TransactionRepository;
import com.poc.CanonicalIngestionEngine.rules.RuleEngine;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import com.poc.CanonicalIngestionEngine.sql.DynamicSqlBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IngestionServiceCoverageTest {

    // =====================================================
    // MOCKS — must match IngestionService constructor
    // =====================================================

    @Mock private RuleEngine ruleEngine;
    @Mock private EventConfigLoader eventConfigLoader;
    @Mock private DataMapper dataMapper;
    @Mock private DynamicSqlBuilder sqlBuilder;
    @Mock private TransactionRepository repository;

    private IngestionService service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // =====================================================
    // SETUP
    // =====================================================

    @BeforeEach
    void setUp() {
        service = new IngestionService(
                new ObjectMapper(),
                ruleEngine,
                eventConfigLoader,
                dataMapper,
                sqlBuilder,
                repository
        );
    }

    // =====================================================
    // BUILDER HELPERS
    // =====================================================

    private EventEnvelope envelope(String event, String operation, String payload) {
        EventEnvelope env = new EventEnvelope();
        env.setEventId(UUID.randomUUID().toString());
        env.setEventName(event);
        env.setEventMetadata("{\"operation\":\"" + operation + "\"}");
        env.setEventPayload(payload);
        return env;
    }

    private EventConfig mainConfig() {
        TableConfig table = new TableConfig();
        table.setTableName("SEND_TRANSACTIONS");
        table.setType("main");
        table.setMapping(Map.of(
                "TRAN_ID", "$.transactionId",
                "STATUS",  "$.status"
        ));
        EventConfig config = new EventConfig();
        config.setEventName("PAYMENT");
        config.setTables(List.of(table));
        return config;
    }

    private EventConfig configWithAddressTable() {
        TableConfig.AddressTypeMapping addr = new TableConfig.AddressTypeMapping();
        addr.setRootPath("$.address");
        addr.setType("HOME");
        addr.setFields(Map.of("CITY", "city"));

        TableConfig mainTable = new TableConfig();
        mainTable.setTableName("SEND_TRANSACTIONS");
        mainTable.setType("main");
        mainTable.setMapping(Map.of("TRAN_ID", "$.transactionId"));

        TableConfig addrTable = new TableConfig();
        addrTable.setTableName("SEND_TRAN_ADDR_DTL");
        addrTable.setType("address");
        addrTable.setParentIdField("TRAN_ID");
        addrTable.setAddressTypes(List.of(addr));

        EventConfig config = new EventConfig();
        config.setEventName("PAYMENT");
        config.setTables(List.of(mainTable, addrTable));
        return config;
    }

    private EventConfig configWithClearingTable() {
        TableConfig clearingTable = new TableConfig();
        clearingTable.setTableName("CLEARING");
        clearingTable.setType("clearing");
        clearingTable.setMapping(Map.of("TRAN_ID", "$.transactionId"));

        TableConfig mainTable = new TableConfig();
        mainTable.setTableName("SEND_TRANSACTIONS");
        mainTable.setType("main");
        mainTable.setMapping(Map.of("TRAN_ID", "$.transactionId"));

        EventConfig config = new EventConfig();
        config.setEventName("PAYMENT");
        config.setTables(List.of(clearingTable, mainTable));
        return config;
    }

    private EventConfig configWithChildTable() {
        TableConfig childTable = new TableConfig();
        childTable.setTableName("SEND_TRAN_DTL");
        childTable.setType("child");
        childTable.setMapping(Map.of("TRAN_ID", "$.transactionId"));

        TableConfig mainTable = new TableConfig();
        mainTable.setTableName("SEND_TRANSACTIONS");
        mainTable.setType("main");
        mainTable.setMapping(Map.of("TRAN_ID", "$.transactionId"));

        EventConfig config = new EventConfig();
        config.setEventName("PAYMENT");
        config.setTables(List.of(childTable, mainTable));
        return config;
    }

    private String validPayload(String tranId) {
        return "{\"transactionId\":\"" + tranId + "\"}";
    }

    // =====================================================
    // 1. isUpdateOperation — INVALID METADATA
    // =====================================================

    @Test
    void isUpdateOperation_invalidMetadata_returnsFalse() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isUpdateOperation", EventEnvelope.class);
        m.setAccessible(true);

        EventEnvelope env = new EventEnvelope();
        env.setEventMetadata("{bad-json}");

        boolean result = (boolean) m.invoke(service, env);
        assertFalse(result);
    }

    @Test
    void isUpdateOperation_operationU_returnsTrue() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isUpdateOperation", EventEnvelope.class);
        m.setAccessible(true);

        EventEnvelope env = new EventEnvelope();
        env.setEventMetadata("{\"operation\":\"U\"}");

        boolean result = (boolean) m.invoke(service, env);
        assertTrue(result);
    }

    @Test
    void isUpdateOperation_operationA_returnsFalse() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isUpdateOperation", EventEnvelope.class);
        m.setAccessible(true);

        EventEnvelope env = new EventEnvelope();
        env.setEventMetadata("{\"operation\":\"A\"}");

        boolean result = (boolean) m.invoke(service, env);
        assertFalse(result);
    }

    // =====================================================
    // 2. ingest — RULE ENGINE IGNORES EVENT
    // =====================================================

    @Test
    void ingest_ignoredByRuleEngine_skipsProcessing() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN_IGN"));

        doAnswer(inv -> {
            EventEnvelope e = inv.getArgument(0);
            e.setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any());

        service.ingest(env);

        verify(repository, never()).insert(anyString(), anyMap());
        verify(repository, never()).updateStatus(anyString(), anyString());
    }

    // =====================================================
    // 3. INSERT FLOW — BASIC SUCCESS
    // =====================================================

    @Test
    void insertFlow_basicSuccess_insertsRecord() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN001"));

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("TRAN_ID", "TXN001")));
        when(repository.exists(any(), any(), any())).thenReturn(false);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT SQL");
        doNothing().when(repository).insert(anyString(), anyMap());

        service.ingest(env);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    // =====================================================
    // 4. INSERT FLOW — EMPTY MAPPED DATA SKIPS INSERT
    // =====================================================

    @Test
    void insertFlow_emptyMappedData_skipsInsert() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN002"));

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>());

        service.ingest(env);

        verify(repository, never()).insert(any(), any());
    }

    // =====================================================
    // 5. INSERT FLOW — NULL MAPPED DATA SKIPS INSERT
    // =====================================================

    @Test
    void insertFlow_nullMappedData_skipsInsert() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN003"));

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(dataMapper.map(any(), any(), any(), anyBoolean())).thenReturn(null);

        service.ingest(env);

        verify(repository, never()).insert(any(), any());
    }

    // =====================================================
    // 6. INSERT FLOW — DUPLICATE TRAN_ID TRIGGERS MERGE
    // =====================================================

    @Test
    void insertFlow_duplicateTranId_triggersMerge() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN004"));

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("TRAN_ID", "TXN004", "STATUS", "INIT")));
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(new HashMap<>());

        service.ingest(env);

        verify(repository, never()).insert(any(), any());
    }

    // =====================================================
    // 7. INSERT FLOW — CLEARING TABLE SKIPPED
    // =====================================================

    @Test
    void insertFlow_clearingTableSkipped() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN005"));

        when(eventConfigLoader.get(any())).thenReturn(configWithClearingTable());
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("TRAN_ID", "TXN005")));
        when(repository.exists(any(), any(), any())).thenReturn(false);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT SQL");
        doNothing().when(repository).insert(anyString(), anyMap());

        service.ingest(env);

        // Only SEND_TRANSACTIONS inserted, clearing skipped
        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    // =====================================================
    // 8. INSERT FLOW — CHILD TABLE SKIPPED
    // =====================================================

    @Test
    void insertFlow_childTableSkipped() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN006"));

        when(eventConfigLoader.get(any())).thenReturn(configWithChildTable());
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("TRAN_ID", "TXN006")));
        when(repository.exists(any(), any(), any())).thenReturn(false);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT SQL");
        doNothing().when(repository).insert(anyString(), anyMap());

        service.ingest(env);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    // =====================================================
    // 9. INSERT FLOW — ADDRESS TABLE PROCESSED
    // =====================================================

    @Test
    void insertFlow_addressTable_processedAfterMain() {
        EventEnvelope env = envelope("PAYMENT", "A",
                "{\"transactionId\":\"TXN007\",\"address\":{\"city\":\"Chennai\"}}");

        when(eventConfigLoader.get(any())).thenReturn(configWithAddressTable());

        // main table mapper
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("TRAN_ID", "TXN007")));

        // address mapper
        Map<String, Object> addrData = new HashMap<>();
        addrData.put("CITY", "Chennai");
        addrData.put("PARENT_ID", "TXN007");
        when(dataMapper.mapAddress(any(), anyString(), anyMap(), anyString(), anyString()))
                .thenReturn(addrData);

        when(repository.exists(any(), any(), any())).thenReturn(false);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("INSERT SQL");
        doNothing().when(repository).insert(anyString(), anyMap());

        service.ingest(env);

        // main + address = 2 inserts
        verify(repository, times(2)).insert(anyString(), anyMap());
    }

    // =====================================================
    // 10. INSERT FLOW — ADDRESS TABLE, NULL PARENT ID SKIPS
    // =====================================================

    @Test
    void insertFlow_addressTable_nullParentId_skipped() {
        // Only an address table, no main table → parentId stays null
        TableConfig.AddressTypeMapping addr = new TableConfig.AddressTypeMapping();
        addr.setRootPath("$.address");
        addr.setType("HOME");
        addr.setFields(Map.of("CITY", "city"));

        TableConfig addrTable = new TableConfig();
        addrTable.setTableName("SEND_TRAN_ADDR_DTL");
        addrTable.setType("address");
        addrTable.setParentIdField("TRAN_ID");
        addrTable.setAddressTypes(List.of(addr));

        EventConfig config = new EventConfig();
        config.setEventName("PAYMENT");
        config.setTables(List.of(addrTable));

        EventEnvelope env = envelope("PAYMENT", "A",
                "{\"transactionId\":\"TXN008\",\"address\":{\"city\":\"Chennai\"}}");

        when(eventConfigLoader.get(any())).thenReturn(config);

        service.ingest(env);

        verify(repository, never()).insert(anyString(), anyMap());
    }

    // =====================================================
    // 11. processAddressInsert — parentId NULL
    // =====================================================

    @Test
    void processAddressInsert_parentIdNull_returnsEarly() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "processAddressInsert", JsonNode.class, TableConfig.class, String.class);
        m.setAccessible(true);

        TableConfig table = new TableConfig();
        m.invoke(service, objectMapper.readTree("{}"), table, null);

        verify(repository, never()).insert(anyString(), anyMap());
    }

    // =====================================================
    // 12. processAddressInsert — addressTypes NULL
    // =====================================================

    @Test
    void processAddressInsert_addressTypesNull_returnsEarly() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "processAddressInsert", JsonNode.class, TableConfig.class, String.class);
        m.setAccessible(true);

        TableConfig table = new TableConfig();
        table.setAddressTypes(null);

        m.invoke(service, objectMapper.readTree("{}"), table, "PARENT_100");

        verify(repository, never()).insert(anyString(), anyMap());
    }

    // =====================================================
    // 13. processAddressInsert — mapped data NULL/EMPTY skips
    // =====================================================

    @Test
    void processAddressInsert_emptyMappedData_skipsInsert() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "processAddressInsert", JsonNode.class, TableConfig.class, String.class);
        m.setAccessible(true);

        TableConfig.AddressTypeMapping addr = new TableConfig.AddressTypeMapping();
        addr.setRootPath("$.address");
        addr.setType("HOME");
        addr.setFields(Map.of("CITY", "city"));

        TableConfig table = new TableConfig();
        table.setTableName("ADDR_TABLE");
        table.setParentIdField("TRAN_ID");
        table.setAddressTypes(List.of(addr));

        when(dataMapper.mapAddress(any(), anyString(), anyMap(), anyString(), anyString()))
                .thenReturn(new HashMap<>());

        m.invoke(service, objectMapper.readTree("{}"), table, "PARENT_100");

        verify(repository, never()).insert(anyString(), anyMap());
    }

    // =====================================================
    // 14. processAddressInsert — FULL SUCCESS
    // =====================================================

    @Test
    void processAddressInsert_success_insertsAddress() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "processAddressInsert", JsonNode.class, TableConfig.class, String.class);
        m.setAccessible(true);

        TableConfig.AddressTypeMapping addr = new TableConfig.AddressTypeMapping();
        addr.setRootPath("$.address");
        addr.setType("HOME");
        addr.setFields(Map.of("CITY", "city"));

        TableConfig table = new TableConfig();
        table.setTableName("ADDR_TABLE");
        table.setParentIdField("TRAN_ID");
        table.setAddressTypes(List.of(addr));

        Map<String, Object> mapped = new HashMap<>();
        mapped.put("CITY", "Chennai");
        mapped.put("PARENT_ID", "PARENT_100");

        when(dataMapper.mapAddress(any(), anyString(), anyMap(), anyString(), anyString()))
                .thenReturn(mapped);
        when(sqlBuilder.buildInsertSql(any(), any(), eq(true))).thenReturn("INSERT ADDR SQL");
        doNothing().when(repository).insert(anyString(), anyMap());

        m.invoke(service, objectMapper.readTree("{}"), table, "PARENT_100");

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    // =====================================================
    // 15. UPDATE FLOW — AIS2 UPDATES SW_SER_NUM
    // =====================================================

    @Test
    void updateFlow_ais2_updatesSwSerNum() {
        EventEnvelope env = envelope("AIS2", "U",
                "{\"accountInformationId\":\"TXN_AIS\",\"switchSerialNumber\":\"SN999\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);

        Map<String, Object> dbRow = new HashMap<>();
        dbRow.put("SW_SER_NUM", null);
        when(repository.findTransaction(any())).thenReturn(dbRow);

        service.ingest(env);

        verify(repository).updateColumn(
                eq("SEND_TRANSACTIONS"),
                eq("TRAN_ID"),
                eq("TXN_AIS"),
                eq("SW_SER_NUM"),
                eq("SN999")
        );
    }

    // =====================================================
    // 16. UPDATE FLOW — AIS2 SKIPS WHEN ALREADY POPULATED
    // =====================================================

    @Test
    void updateFlow_ais2_skipsWhenAlreadyPopulated() {
        EventEnvelope env = envelope("AIS2", "U",
                "{\"accountInformationId\":\"TXN_AIS2\",\"switchSerialNumber\":\"SN999\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(Map.of("SW_SER_NUM", "EXISTING"));

        service.ingest(env);

        verify(repository, never()).updateColumn(any(), any(), any(), eq("SW_SER_NUM"), any());
    }

    // =====================================================
    // 17. UPDATE FLOW — AIS2 NULL EXISTING TX THROWS
    // =====================================================

    @Test
    void updateFlow_ais2_nullExistingTransaction_throws() {
        EventEnvelope env = envelope("AIS2", "U",
                "{\"accountInformationId\":\"TXN_AIS3\",\"switchSerialNumber\":\"SN999\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(null);

        assertThrows(IngestionService.IngestionProcessingException.class,
                () -> service.ingest(env));
    }

    // =====================================================
    // 18. UPDATE FLOW — TRANSACTION NOT FOUND THROWS
    // =====================================================

    @Test
    void updateFlow_transactionNotFound_throws() {
        EventEnvelope env = envelope("CLEARING", "U",
                "{\"transactionId\":\"TXN_NF\",\"status\":\"SETTLED\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(false);

        assertThrows(IngestionService.IngestionProcessingException.class,
                () -> service.ingest(env));
    }

    // =====================================================
    // 19. UPDATE FLOW — NO TRANSACTION ID IN PAYLOAD THROWS
    // =====================================================

    @Test
    void updateFlow_noTransactionId_throws() {
        EventEnvelope env = envelope("CLEARING", "U",
                "{\"someOtherField\":\"value\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());

        assertThrows(IngestionService.IngestionProcessingException.class,
                () -> service.ingest(env));
    }

    // =====================================================
    // 20. UPDATE FLOW — STATUS TRANSITION BLOCKED
    // =====================================================

    @Test
    void updateFlow_statusTransitionBlocked_skipsUpdate() {
        EventEnvelope env = envelope("CLEARING", "U",
                "{\"transactionId\":\"TXN_BLK\",\"status\":\"STARTED\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(Map.of("STATUS", "SETTLED"));

        service.ingest(env);

        verify(repository, never()).updateStatus(any(), any());
    }

    // =====================================================
    // 21. UPDATE FLOW — STATUS TRANSITION ALLOWED, FULL UPDATE
    // =====================================================

    @Test
    void updateFlow_statusTransitionAllowed_updatesStatus() {
        EventEnvelope env = envelope("CLEARING", "U",
                "{\"transactionId\":\"TXN_UPD\",\"status\":\"SETTLED\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(
                new HashMap<>(Map.of("STATUS", "INIT")));
        when(repository.findAllRelatedData(any())).thenReturn(new HashMap<>());
        when(repository.columnExists(any(), any())).thenReturn(true);
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("STATUS", "SETTLED")));

        service.ingest(env);

        verify(repository).updateStatus("TXN_UPD", "SETTLED");
    }

    // =====================================================
    // 22. UPDATE FLOW — NO INCOMING STATUS, SKIP STATUS UPDATE
    // =====================================================

    @Test
    void updateFlow_noIncomingStatus_skipsStatusUpdate() {
        EventEnvelope env = envelope("CLEARING", "U",
                "{\"transactionId\":\"TXN_NOSTA\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(new HashMap<>());
        when(repository.findAllRelatedData(any())).thenReturn(new HashMap<>());
        when(repository.columnExists(any(), any())).thenReturn(true);
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("TRAN_ID", "TXN_NOSTA")));

        service.ingest(env);

        verify(repository, never()).updateStatus(any(), any());
    }

    // =====================================================
    // 23. UPDATE FLOW — TRANSACTION ID FROM DIFFERENT FIELDS
    // =====================================================

    @Test
    void updateFlow_resolvesTransactionIdFromPaymentTransactionId() {
        EventEnvelope env = envelope("CLEARING", "U",
                "{\"paymentTransactionId\":\"TXN_PT\",\"status\":\"SETTLED\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(new HashMap<>(Map.of("STATUS","INIT")));
        when(repository.findAllRelatedData(any())).thenReturn(new HashMap<>());
        when(repository.columnExists(any(), any())).thenReturn(true);
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("STATUS","SETTLED")));

        service.ingest(env);

        verify(repository).updateStatus("TXN_PT", "SETTLED");
    }

    @Test
    void updateFlow_resolvesTransactionIdFromAuthorizationId() {
        EventEnvelope env = envelope("CLEARING", "U",
                "{\"authorizationId\":\"TXN_AUTH\",\"status\":\"COMPLETED\"}");

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(new HashMap<>(Map.of("STATUS","INIT")));
        when(repository.findAllRelatedData(any())).thenReturn(new HashMap<>());
        when(repository.columnExists(any(), any())).thenReturn(true);
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of("STATUS","COMPLETED")));

        service.ingest(env);

        verify(repository).updateStatus("TXN_AUTH", "COMPLETED");
    }

    // =====================================================
    // 24. isStatusTransitionAllowed — BLANK TRAN_ID ALLOWS
    // =====================================================

    @Test
    void isStatusTransitionAllowed_blankTranId_allows() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        boolean result = (boolean) m.invoke(service, "", "SETTLED");
        assertTrue(result);
    }

    @Test
    void isStatusTransitionAllowed_nullTranId_allows() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        boolean result = (boolean) m.invoke(service, null, "SETTLED");
        assertTrue(result);
    }

    // =====================================================
    // 25. isStatusTransitionAllowed — NULL INCOMING STATUS ALLOWS
    // =====================================================

    @Test
    void isStatusTransitionAllowed_nullIncomingStatus_allows() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        boolean result = (boolean) m.invoke(service, "TXN_X", null);
        assertTrue(result);
    }

    // =====================================================
    // 26. isStatusTransitionAllowed — RECORD DOES NOT EXIST
    // =====================================================

    @Test
    void isStatusTransitionAllowed_recordNotExists_allows() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        when(repository.exists(any(), any(), any())).thenReturn(false);

        boolean result = (boolean) m.invoke(service, "TXN_X", "SETTLED");
        assertTrue(result);
    }

    // =====================================================
    // 27. isStatusTransitionAllowed — NULL ROW ALLOWS
    // =====================================================

    @Test
    void isStatusTransitionAllowed_existingRowNull_allows() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(null);

        boolean result = (boolean) m.invoke(service, "TXN_X", "SETTLED");
        assertTrue(result);
    }

    // =====================================================
    // 28. isStatusTransitionAllowed — NULL CURRENT STATUS ALLOWS
    // =====================================================

    @Test
    void isStatusTransitionAllowed_nullCurrentStatus_allows() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        Map<String, Object> row = new HashMap<>();
        row.put("STATUS", null);
        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(row);

        boolean result = (boolean) m.invoke(service, "TXN_X", "SETTLED");
        assertTrue(result);
    }

    // =====================================================
    // 29. isStatusTransitionAllowed — TERMINAL → LOW PRIORITY BLOCKS
    // =====================================================

    @Test
    void isStatusTransitionAllowed_terminalToLowPriority_blocks() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(Map.of("STATUS", "APPROVED"));

        boolean result = (boolean) m.invoke(service, "TXN_X", "PENDING");
        assertFalse(result);
    }

    @Test
    void isStatusTransitionAllowed_terminalToTerminal_allows() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        when(repository.exists(any(), any(), any())).thenReturn(true);
        when(repository.findTransaction(any())).thenReturn(Map.of("STATUS", "SETTLED"));

        boolean result = (boolean) m.invoke(service, "TXN_X", "COMPLETED");
        assertTrue(result);
    }

    // =====================================================
    // 30. isStatusTransitionAllowed — DB EXCEPTION ALLOWS BY DEFAULT
    // =====================================================

    @Test
    void isStatusTransitionAllowed_dbException_allowsByDefault() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "isStatusTransitionAllowed", String.class, String.class);
        m.setAccessible(true);

        when(repository.exists(any(), any(), any()))
                .thenThrow(new RuntimeException("DB_DOWN"));

        boolean result = (boolean) m.invoke(service, "TXN_X", "SETTLED");
        assertTrue(result);
    }

    // =====================================================
    // 31. mergeNullFields — UNKNOWN COLUMN SKIPPED
    // =====================================================

    @Test
    void mergeNullFields_unknownColumn_skipped() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "mergeNullFields", String.class, String.class, Map.class);
        m.setAccessible(true);

        Map<String, Object> incoming = new HashMap<>();
        incoming.put("BAD_COL", "value");

        when(repository.findTransaction(any())).thenReturn(new HashMap<>());
        when(repository.columnExists(any(), any())).thenReturn(false);

        m.invoke(service, "SEND_TRANSACTIONS", "TXN_M1", incoming);

        verify(repository, never()).updateColumn(any(), any(), any(), any(), any());
    }

    // =====================================================
    // 32. mergeNullFields — EXISTING VALUE NON-NULL SKIPS UPDATE
    // =====================================================

    @Test
    void mergeNullFields_existingValueNotNull_skipsUpdate() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "mergeNullFields", String.class, String.class, Map.class);
        m.setAccessible(true);

        Map<String, Object> incoming = new HashMap<>();
        incoming.put("STATUS", "SETTLED");

        when(repository.findTransaction(any()))
                .thenReturn(new HashMap<>(Map.of("STATUS", "EXISTING_VAL")));
        when(repository.columnExists(any(), any())).thenReturn(true);

        m.invoke(service, "SEND_TRANSACTIONS", "TXN_M2", incoming);

        verify(repository, never()).updateColumn(any(), any(), any(), any(), any());
    }

    // =====================================================
    // 33. mergeNullFields — EXISTING NULL, INCOMING VALUE → UPDATE
    // =====================================================

    @Test
    void mergeNullFields_existingNull_incomingValue_updates() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "mergeNullFields", String.class, String.class, Map.class);
        m.setAccessible(true);

        Map<String, Object> incoming = new HashMap<>();
        incoming.put("STATUS", "SETTLED");

        Map<String, Object> existing = new HashMap<>();
        existing.put("STATUS", null);

        when(repository.findTransaction(any())).thenReturn(existing);
        when(repository.columnExists(any(), any())).thenReturn(true);

        m.invoke(service, "SEND_TRANSACTIONS", "TXN_M3", incoming);

        verify(repository).updateColumn(
                eq("SEND_TRANSACTIONS"),
                eq("TRAN_ID"),
                eq("TXN_M3"),
                eq("STATUS"),
                eq("SETTLED")
        );
    }

    // =====================================================
    // 34. mergeNullFields — EMPTY EXISTING ROW, RETURNS EARLY
    // =====================================================

    @Test
    void mergeNullFields_emptyExistingRow_returnsEarly() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "mergeNullFields", String.class, String.class, Map.class);
        m.setAccessible(true);

        when(repository.findTransaction(any())).thenReturn(new HashMap<>());

        Map<String, Object> incoming = new HashMap<>();
        incoming.put("STATUS", "SETTLED");

        m.invoke(service, "SEND_TRANSACTIONS", "TXN_M4", incoming);

        // Empty existing row → method returns early → no update
        verify(repository, never()).updateColumn(any(), any(), any(), any(), any());
    }

    // =====================================================
    // 35. applyDefaults — ALL DEFAULTS SET
    // =====================================================

    @Test
    void applyDefaults_allDefaultsApplied() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "applyDefaults", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_CRTE_DT", "2025-01-01");

        m.invoke(service, data);

        assertEquals("00", data.get("CUR_STAT"));
        assertEquals(0, data.get("NON_FIN_TXN"));
        assertEquals("2025-01-01", data.get("RPLCTN_UPDT_TS"));
    }

    @Test
    void applyDefaults_existingValuesNotOverwritten() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "applyDefaults", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("CUR_STAT", "99");
        data.put("NON_FIN_TXN", 1);
        data.put("RPLCTN_UPDT_TS", "EXISTING");

        m.invoke(service, data);

        assertEquals("99", data.get("CUR_STAT"));
        assertEquals(1, data.get("NON_FIN_TXN"));
        assertEquals("EXISTING", data.get("RPLCTN_UPDT_TS"));
    }

    // =====================================================
    // 36. validateAndConvertTypes — VALID NON_FIN_TXN
    // =====================================================

    @Test
    void validateAndConvertTypes_validNonFinTxn_parsedAsInt() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "validateAndConvertTypes", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("NON_FIN_TXN", "5");

        m.invoke(service, data);

        assertEquals(5, data.get("NON_FIN_TXN"));
    }

    @Test
    void validateAndConvertTypes_invalidNonFinTxn_defaultsZero() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "validateAndConvertTypes", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("NON_FIN_TXN", "NOT_A_NUMBER");

        m.invoke(service, data);

        assertEquals(0, data.get("NON_FIN_TXN"));
    }

    // =====================================================
    // 37. validateAndConvertTypes — VALID TRAN_AMT
    // =====================================================

    @Test
    void validateAndConvertTypes_validTranAmt_parsedAsDouble() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "validateAndConvertTypes", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_AMT", "100.50");

        m.invoke(service, data);

        assertEquals(100.50, data.get("TRAN_AMT"));
    }

    @Test
    void validateAndConvertTypes_invalidTranAmt_removed() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "validateAndConvertTypes", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_AMT", "INVALID");

        m.invoke(service, data);

        assertFalse(data.containsKey("TRAN_AMT"));
    }

    @Test
    void validateAndConvertTypes_negativeTranAmt_throws() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "validateAndConvertTypes", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_AMT", "-50.0");

        assertThrows(Exception.class, () -> m.invoke(service, data));
    }

    // =====================================================
    // 38. extractParentId — TRAN_ID PRESENT
    // =====================================================

    @Test
    void extractParentId_tranIdPresent_returnsIt() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "extractParentId", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_ID", "TXN_PARENT");

        String result = (String) m.invoke(service, data);
        assertEquals("TXN_PARENT", result);
    }

    @Test
    void extractParentId_tranIdNull_generatesUuid() throws Exception {
        Method m = IngestionService.class.getDeclaredMethod(
                "extractParentId", Map.class);
        m.setAccessible(true);

        Map<String, Object> data = new HashMap<>();

        String result = (String) m.invoke(service, data);
        assertNotNull(result);
        assertFalse(result.isBlank());
    }

    // =====================================================
    // 39. getConfig — MISSING CONFIG THROWS
    // =====================================================

    @Test
    void getConfig_missingConfig_throws() {
        EventEnvelope env = envelope("UNKNOWN_EVENT", "A", "{}");
        when(eventConfigLoader.get(any())).thenReturn(null);

        assertThrows(IngestionService.IngestionProcessingException.class,
                () -> service.getConfig(env));
    }

    @Test
    void getConfig_validConfig_returnsConfig() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN_CFG"));
        when(eventConfigLoader.get(any())).thenReturn(mainConfig());

        EventConfig config = service.getConfig(env);
        assertNotNull(config);
        assertEquals("PAYMENT", config.getEventName());
    }

    // =====================================================
    // 40. ingest — INVALID JSON PAYLOAD THROWS
    // =====================================================

    @Test
    void ingest_invalidJsonPayload_throws() {
        EventEnvelope env = envelope("PAYMENT", "A", "{invalid_json}");
        when(eventConfigLoader.get(any())).thenReturn(mainConfig());

        assertThrows(IngestionService.IngestionProcessingException.class,
                () -> service.ingest(env));
    }

    // =====================================================
    // 41. INSERT FLOW — SEND_TRANSACTIONS STATUS FORCED TO INIT
    // =====================================================

    @Test
    void insertFlow_sendTransactions_statusForcedToInit() {
        EventEnvelope env = envelope("PAYMENT", "A", validPayload("TXN_INIT"));

        when(eventConfigLoader.get(any())).thenReturn(mainConfig());
        Map<String, Object> mapped = new HashMap<>(Map.of("TRAN_ID", "TXN_INIT"));
        when(dataMapper.map(any(), any(), any(), anyBoolean())).thenReturn(mapped);
        when(repository.exists(any(), any(), any())).thenReturn(false);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean())).thenReturn("SQL");
        doNothing().when(repository).insert(anyString(), anyMap());

        service.ingest(env);

        assertEquals("INIT", mapped.get("STATUS"));
    }

    // =====================================================
    // 42. IngestionProcessingException — MESSAGES
    // =====================================================

    @Test
    void ingestionProcessingException_messageOnly() {
        IngestionService.IngestionProcessingException ex =
                new IngestionService.IngestionProcessingException("TEST_MESSAGE");
        assertEquals("TEST_MESSAGE", ex.getMessage());
    }

    @Test
    void ingestionProcessingException_messageAndCause() {
        RuntimeException cause = new RuntimeException("ROOT_CAUSE");
        IngestionService.IngestionProcessingException ex =
                new IngestionService.IngestionProcessingException("MSG", cause);
        assertEquals("MSG", ex.getMessage());
        assertEquals(cause, ex.getCause());
    }
}