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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IngestionServiceTest {

    @Mock
    private RuleEngine ruleEngine;

    @Mock
    private EventConfigLoader eventConfigLoader;

    @Mock
    private DataMapper dataMapper;

    @Mock
    private DynamicSqlBuilder sqlBuilder;

    @Mock
    private TransactionRepository repository;

    private IngestionService ingestionService;
    private ObjectMapper objectMapper;

    private final String validJson = """
{
  "eventName":"AVS",
  "eventId":"evt-001",
  "eventSource":"AVS_SERVICE",
  "correlationId":"corr-001",
  "eventTimestamp":1738656000000,
  "eventMetadata":"{\\"operation\\":\\"I\\"}",
  "eventPayload":"{\\"avsTranId\\":\\"TXN-1\\",\\"transactionType\\":\\"AVS\\"}"
}
""";

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        ingestionService = new IngestionService(
                objectMapper,
                ruleEngine,
                eventConfigLoader,
                dataMapper,
                sqlBuilder,
                repository
        );
    }

    // ================= SUCCESS FLOW =================
    @Test
    void shouldSuccessfullyIngestAVS() throws Exception {
        EventConfig config = createEventConfig();
        Map<String, Object> mappedData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(JsonNode.class), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mappedData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());
        doNothing().when(repository).insert(anyString(), anyMap());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    // ================= RULE IGNORE =================
    @Test
    void shouldSkipWhenIgnoredByRules() throws Exception {
        doAnswer(invocation -> {
            EventEnvelope envelope = invocation.getArgument(0);
            envelope.setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, never()).insert(anyString(), anyMap());
    }

    // ================= CONFIG MISSING =================
    @Test
    void shouldThrowWhenConfigMissing() throws Exception {
        when(eventConfigLoader.get("AVS")).thenReturn(null);
        doNothing().when(ruleEngine).apply(any());

        assertThrows(RuntimeException.class, () -> ingestionService.ingest(validJson));

        verify(repository, never()).insert(anyString(), anyMap());
    }

    // ================= MULTI TABLE =================
    @Test
    void shouldProcessMultipleTables() throws Exception {
        EventConfig config = createMultiTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> detailData = createDetailData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(detailData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());
        doNothing().when(repository).insert(anyString(), anyMap());

        ingestionService.ingest(validJson);

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    // ================= ADDRESS TABLE TESTS =================

    @Test
    void shouldProcessAddressTableWithValidData() throws Exception {
        EventConfig config = createAddressTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> addressData = createAddressData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(addressData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());
        doNothing().when(repository).insert(anyString(), anyMap());

        ingestionService.ingest(validJson);

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipAddressTableWhenDataMapperReturnsEmpty() throws Exception {
        EventConfig config = createAddressTableConfig();
        Map<String, Object> mainData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(Collections.emptyMap());
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(eq("INSERT SQL"), eq(mainData));
    }

    @Test
    void shouldSkipAddressTableWhenDataMapperReturnsNull() throws Exception {
        EventConfig config = createAddressTableConfig();
        Map<String, Object> mainData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(null);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipAddressTableWhenSqlBuilderReturnsNull() throws Exception {
        EventConfig config = createAddressTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> addressData = createAddressData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(addressData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL")
                .thenReturn(null);

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipAddressTableWhenSqlBuilderReturnsEmpty() throws Exception {
        EventConfig config = createAddressTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> addressData = createAddressData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(addressData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL")
                .thenReturn("");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    // ================= REGULAR TABLE TESTS =================

    @Test
    void shouldProcessRegularTableWithValidData() throws Exception {
        EventConfig config = createRegularTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> regularData = createRegularData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(regularData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());
        doNothing().when(repository).insert(anyString(), anyMap());

        ingestionService.ingest(validJson);

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipRegularTableWhenDataMapperReturnsEmpty() throws Exception {
        EventConfig config = createRegularTableConfig();
        Map<String, Object> mainData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(Collections.emptyMap());
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipRegularTableWhenDataMapperReturnsNull() throws Exception {
        EventConfig config = createRegularTableConfig();
        Map<String, Object> mainData = createMappedData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(null);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipRegularTableWhenSqlBuilderReturnsNull() throws Exception {
        EventConfig config = createRegularTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> regularData = createRegularData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(regularData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL")
                .thenReturn(null);

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipRegularTableWhenSqlBuilderReturnsEmpty() throws Exception {
        EventConfig config = createRegularTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> regularData = createRegularData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(regularData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL")
                .thenReturn("");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, times(1)).insert(anyString(), anyMap());
    }

    // ================= EXTRACT PARENT ID TESTS =================

    @Test
    void shouldExtractParentIdSuccessfully() throws Exception {
        EventConfig config = createMultiTableConfig();
        Map<String, Object> mainData = createMappedData();
        Map<String, Object> detailData = createDetailData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(detailData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    @Test
    void shouldHandleNullParentId() throws Exception {
        EventConfig config = createEventConfig();

        TableConfig childTable = new TableConfig();
        childTable.setTableName("CHILD_TABLE");
        childTable.setType("detail");
        childTable.setOrder(2);
        childTable.setParentIdField("MISSING_FIELD");

        Map<String, String> mapping = new HashMap<>();
        mapping.put("CHILD_ID", "$.childId");
        childTable.setMapping(mapping);

        List<TableConfig> tables = new ArrayList<>(config.getTables());
        tables.add(childTable);
        config.setTables(tables);

        Map<String, Object> mainData = createMappedData();
        Map<String, Object> childData = new HashMap<>();
        childData.put("CHILD_ID", "CHILD-1");

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(childData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    @Test
    void shouldHandleEmptyParentId() throws Exception {
        EventConfig config = createEventConfig();

        // Add empty TRAN_ID to main data
        Map<String, Object> mainData = new HashMap<>();
        mainData.put("TRAN_ID", "");
        mainData.put("TRAN_TYPE", "AVS");

        TableConfig childTable = new TableConfig();
        childTable.setTableName("CHILD_TABLE");
        childTable.setType("detail");
        childTable.setOrder(2);
        childTable.setParentIdField("TRAN_ID");

        Map<String, String> mapping = new HashMap<>();
        mapping.put("CHILD_ID", "$.childId");
        childTable.setMapping(mapping);

        List<TableConfig> tables = new ArrayList<>(config.getTables());
        tables.add(childTable);
        config.setTables(tables);

        Map<String, Object> childData = new HashMap<>();
        childData.put("CHILD_ID", "CHILD-1");

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(childData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    // ================= EMPTY & NULL DATA =================

    @Test
    void shouldSkipMainTableWhenMappedDataIsEmpty() throws Exception {
        EventConfig config = createEventConfig();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(Collections.emptyMap());
        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, never()).insert(anyString(), anyMap());
        verify(sqlBuilder, never()).buildInsertSql(anyString(), anySet(), anyBoolean());
    }

    @Test
    void shouldSkipMainTableWhenMappedDataIsNull() throws Exception {
        EventConfig config = createEventConfig();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(null);
        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, never()).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipMainTableWhenSqlIsNull() throws Exception {
        EventConfig config = createEventConfig();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(createMappedData());
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn(null);
        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, never()).insert(anyString(), anyMap());
    }

    @Test
    void shouldSkipMainTableWhenSqlIsEmpty() throws Exception {
        EventConfig config = createEventConfig();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(createMappedData());
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("");
        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(sqlBuilder, times(1)).buildInsertSql(anyString(), anySet(), anyBoolean());
    }

    // ================= INVALID JSON =================

    @Test
    void shouldThrowForInvalidJson() {
        String invalidJson = "{invalid-json";
        assertThrows(Exception.class, () -> ingestionService.ingest(invalidJson));
    }

    @Test
    void shouldThrowForEmptyJson() {
        assertThrows(Exception.class, () -> ingestionService.ingest(""));
    }

    @Test
    void shouldThrowForNullJson() {
        assertThrows(Exception.class, () -> ingestionService.ingest(null));
    }

    @Test
    void shouldThrowForMissingEventName() {
        String jsonWithoutEventName = """
{
  "eventId":"evt-001",
  "eventSource":"AVS_SERVICE"
}
""";
        assertThrows(Exception.class, () -> ingestionService.ingest(jsonWithoutEventName));
    }

    // ================= MIXED SCENARIOS =================

    @Test
    void shouldProcessMixedTablesWithSomeSkipped() throws Exception {
        EventConfig config = createComplexConfig();

        Map<String, Object> mainData = createMappedData();
        Map<String, Object> regularData = createRegularData();
        Map<String, Object> addressData = createAddressData();

        when(eventConfigLoader.get("AVS")).thenReturn(config);
        when(dataMapper.map(any(), anyMap(), anyList(), anyBoolean()))
                .thenReturn(mainData)
                .thenReturn(regularData)
                .thenReturn(addressData);
        when(sqlBuilder.buildInsertSql(anyString(), anySet(), anyBoolean()))
                .thenReturn("INSERT SQL");

        doNothing().when(ruleEngine).apply(any());

        ingestionService.ingest(validJson);

        verify(repository, atLeastOnce()).insert(anyString(), anyMap());
    }

    // ================= HELPER METHODS =================

    private EventConfig createEventConfig() {
        EventConfig config = new EventConfig();
        config.setEventName("AVS");

        TableConfig mainTable = new TableConfig();
        mainTable.setTableName("SEND_TRANSACTIONS");
        mainTable.setType("main");
        mainTable.setOrder(1);
        mainTable.setAutoGenerateId(false);

        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("TRAN_TYPE", "$.transactionType");
        mainTable.setMapping(mapping);

        mainTable.setMandatory(Arrays.asList("TRAN_ID", "TRAN_TYPE"));
        config.setTables(Arrays.asList(mainTable));
        return config;
    }

    private EventConfig createMultiTableConfig() {
        EventConfig config = createEventConfig();

        TableConfig detailTable = new TableConfig();
        detailTable.setTableName("SEND_TRAN_DTL");
        detailTable.setType("detail");
        detailTable.setOrder(2);
        detailTable.setParentIdField("TRAN_ID");

        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("PAYMT_REF", "$.avsExtRefId");
        detailTable.setMapping(mapping);

        List<TableConfig> tables = new ArrayList<>(config.getTables());
        tables.add(detailTable);
        config.setTables(tables);
        return config;
    }

    private EventConfig createAddressTableConfig() {
        EventConfig config = createEventConfig();

        TableConfig addressTable = new TableConfig();
        addressTable.setTableName("SEND_TRAN_ADDR_DTL");
        addressTable.setType("address");
        addressTable.setOrder(2);
        addressTable.setParentIdField("TRAN_ID");

        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("ADDR_LINE1", "$.addrLine1");
        mapping.put("CITY", "$.city");
        addressTable.setMapping(mapping);

        List<TableConfig> tables = new ArrayList<>(config.getTables());
        tables.add(addressTable);
        config.setTables(tables);
        return config;
    }

    private EventConfig createRegularTableConfig() {
        EventConfig config = createEventConfig();

        TableConfig regularTable = new TableConfig();
        regularTable.setTableName("SEND_TRAN_REGULAR");
        regularTable.setType("regular");
        regularTable.setOrder(2);
        regularTable.setParentIdField("TRAN_ID");

        Map<String, String> mapping = new HashMap<>();
        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("DESCRIPTION", "$.description");
        regularTable.setMapping(mapping);

        List<TableConfig> tables = new ArrayList<>(config.getTables());
        tables.add(regularTable);
        config.setTables(tables);
        return config;
    }

    private EventConfig createComplexConfig() {
        EventConfig config = createEventConfig();

        // Add regular table
        TableConfig regularTable = new TableConfig();
        regularTable.setTableName("SEND_TRAN_REGULAR");
        regularTable.setType("regular");
        regularTable.setOrder(2);
        regularTable.setParentIdField("TRAN_ID");
        Map<String, String> regularMapping = new HashMap<>();
        regularMapping.put("TRAN_ID", "$.avsTranId");
        regularTable.setMapping(regularMapping);

        // Add address table
        TableConfig addressTable = new TableConfig();
        addressTable.setTableName("SEND_TRAN_ADDR_DTL");
        addressTable.setType("address");
        addressTable.setOrder(3);
        addressTable.setParentIdField("TRAN_ID");
        Map<String, String> addressMapping = new HashMap<>();
        addressMapping.put("TRAN_ID", "$.avsTranId");
        addressTable.setMapping(addressMapping);

        List<TableConfig> tables = new ArrayList<>(config.getTables());
        tables.add(regularTable);
        tables.add(addressTable);
        config.setTables(tables);
        return config;
    }

    private Map<String, Object> createMappedData() {
        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_ID", "TXN-1");
        data.put("TRAN_TYPE", "AVS");
        return data;
    }

    private Map<String, Object> createDetailData() {
        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_ID", "TXN-1");
        data.put("PAYMT_REF", "REF-001");
        return data;
    }

    private Map<String, Object> createAddressData() {
        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_ID", "TXN-1");
        data.put("ADDR_LINE1", "123 Main St");
        data.put("CITY", "Chennai");
        return data;
    }

    private Map<String, Object> createRegularData() {
        Map<String, Object> data = new HashMap<>();
        data.put("TRAN_ID", "TXN-1");
        data.put("DESCRIPTION", "Regular transaction");
        return data;
    }
}