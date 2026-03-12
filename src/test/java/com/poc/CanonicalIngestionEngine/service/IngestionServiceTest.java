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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IngestionServiceTest {

    @Mock private ObjectMapper mapper;
    @Mock private RuleEngine ruleEngine;
    @Mock private EventConfigLoader loader;
    @Mock private DataMapper dataMapper;
    @Mock private DynamicSqlBuilder sqlBuilder;
    @Mock private TransactionRepository repository;

    private IngestionService service;

    private static final ObjectMapper REAL = new ObjectMapper();

    private static final String META_INSERT = "{\"operation\":\"A\"}";
    private static final String META_UPDATE = "{\"operation\":\"U\"}";
    private static final String PAYLOAD = "{\"paymentTransactionId\":\"PAY123\"}";

    @BeforeEach
    void setup() {
        service = new IngestionService(
                mapper, ruleEngine, loader,
                dataMapper, sqlBuilder, repository
        );
    }

    private EventEnvelope env(String meta) {
        EventEnvelope e = new EventEnvelope();
        e.setEventId("1");
        e.setEventName("PAYMENT");
        e.setEventPayload(PAYLOAD);
        e.setEventMetadata(meta);
        return e;
    }

    private TableConfig mainTable() {
        TableConfig t = new TableConfig();
        t.setTableName("SEND_TRANSACTIONS");
        t.setType("main");
        t.setMapping(new HashMap<>());
        return t;
    }

    private EventConfig config(TableConfig... tables) {
        EventConfig c = new EventConfig();
        c.setTables(Arrays.asList(tables));
        return c;
    }

    private void mockJson(String meta) throws Exception {
        JsonNode metaNode = REAL.readTree(meta);
        JsonNode payloadNode = REAL.readTree(PAYLOAD);

        when(mapper.readTree(meta)).thenReturn(metaNode);
        when(mapper.readTree(PAYLOAD)).thenReturn(payloadNode);
    }

    // helper to create mutable data map
    private Map<String,Object> tranMap() {
        Map<String,Object> m = new HashMap<>();
        m.put("TRAN_ID","PAY123");
        return m;
    }

    // ================= IGNORE =================

    @Test
    void shouldIgnoreEvent() {

        EventEnvelope e = env(META_INSERT);

        doAnswer(i -> {
            ((EventEnvelope) i.getArgument(0)).setIgnore(true);
            return null;
        }).when(ruleEngine).apply(any());

        service.ingest(e);

        verify(repository, never()).insert(any(), any());
    }

    // ================= INSERT =================

    @Test
    void shouldInsertRecord() throws Exception {

        EventEnvelope e = env(META_INSERT);
        mockJson(META_INSERT);

        when(loader.get("PAYMENT")).thenReturn(config(mainTable()));
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(tranMap());

        when(repository.exists(any(), any(), any())).thenReturn(false);
        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean()))
                .thenReturn("SQL");

        service.ingest(e);

        verify(repository).insert(any(), any());
    }

    @Test
    void shouldSkipDuplicateInsert() throws Exception {

        EventEnvelope e = env(META_INSERT);
        mockJson(META_INSERT);

        when(loader.get("PAYMENT")).thenReturn(config(mainTable()));
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(tranMap());

        when(repository.exists(any(), any(), any())).thenReturn(true);

        service.ingest(e);

        verify(repository, never()).insert(any(), any());
    }

    // ================= INSERT ERROR =================

    @Test
    void insertFlow_shouldWrapException() throws Exception {

        EventEnvelope e = env(META_INSERT);
        mockJson(META_INSERT);

        when(loader.get("PAYMENT")).thenReturn(config(mainTable()));

        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenThrow(new RuntimeException("mapping failed"));

        assertThrows(IngestionService.IngestionProcessingException.class,
                () -> service.ingest(e));
    }

    // ================= ADDRESS EMPTY =================

    @Test
    void addressInsert_shouldSkipWhenEmpty() throws Exception {

        EventEnvelope e = env(META_INSERT);
        mockJson(META_INSERT);

        TableConfig main = mainTable();

        TableConfig address = new TableConfig();
        address.setType("address");
        address.setTableName("ADDR");
        address.setParentIdField("TRAN_ID");

        TableConfig.AddressTypeMapping mapping =
                new TableConfig.AddressTypeMapping();
        mapping.setType("HOME");
        mapping.setRootPath("root");
        mapping.setFields(new HashMap<>());

        address.setAddressTypes(List.of(mapping));

        when(loader.get("PAYMENT")).thenReturn(config(main,address));
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(tranMap());

        when(dataMapper.mapAddress(any(),any(),any(),any(),any()))
                .thenReturn(Collections.emptyMap());

        service.ingest(e);

        verify(repository, never()).insert(eq("ADDR"), any());
    }

    // ================= ADDRESS EXISTS =================

    @Test
    void addressInsert_shouldSkipIfExists() throws Exception {

        EventEnvelope e = env(META_INSERT);
        mockJson(META_INSERT);

        TableConfig main = mainTable();

        TableConfig address = new TableConfig();
        address.setType("address");
        address.setTableName("ADDR");
        address.setParentIdField("TRAN_ID");

        TableConfig.AddressTypeMapping mapping =
                new TableConfig.AddressTypeMapping();
        mapping.setType("HOME");
        mapping.setRootPath("root");
        mapping.setFields(new HashMap<>());

        address.setAddressTypes(List.of(mapping));

        Map<String,Object> addrData = new HashMap<>();
        addrData.put("PARENT_ID","PAY123");

        when(loader.get("PAYMENT")).thenReturn(config(main,address));
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(tranMap());

        when(dataMapper.mapAddress(any(),any(),any(),any(),any()))
                .thenReturn(addrData);

        when(repository.existsWithType(any(),any(),any(),any(),any()))
                .thenReturn(true);

        service.ingest(e);

        verify(repository, never()).insert(eq("ADDR"), any());
    }

    // ================= UPDATE =================

    @Test
    void shouldUpdateExistingRow() throws Exception {

        EventEnvelope e = env(META_UPDATE);
        mockJson(META_UPDATE);

        when(loader.get("PAYMENT")).thenReturn(config(mainTable()));
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(tranMap());

        when(repository.exists(any(),any(),any())).thenReturn(true);
        when(sqlBuilder.buildUpdateSql(any(),any(),any())).thenReturn("SQL");

        service.ingest(e);

        verify(repository).update(any(), any());
    }

    @Test
    void update_shouldSkipAddressTable() throws Exception {

        EventEnvelope e = env(META_UPDATE);
        mockJson(META_UPDATE);

        TableConfig address = new TableConfig();
        address.setType("address");

        when(loader.get("PAYMENT")).thenReturn(config(address));

        service.ingest(e);

        verify(repository, never()).update(any(), any());
    }

    // ================= UPDATE EMPTY DATA =================

    @Test
    void update_shouldSkipWhenDataEmpty() throws Exception {

        EventEnvelope e = env(META_UPDATE);
        mockJson(META_UPDATE);

        when(loader.get("PAYMENT")).thenReturn(config(mainTable()));
        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(Collections.emptyMap());

        service.ingest(e);

        verify(repository, never()).update(any(), any());
    }

    // ================= UPDATE NULL TRAN =================

    @Test
    void update_shouldSkipWhenTranIdNull() throws Exception {

        EventEnvelope e = env(META_UPDATE);
        mockJson(META_UPDATE);

        Map<String,Object> data = new HashMap<>();

        when(loader.get("PAYMENT")).thenReturn(config(mainTable()));
        when(dataMapper.map(any(), any(), any(), anyBoolean())).thenReturn(data);

        service.ingest(e);

        verify(repository, never()).update(any(), any());
    }

    // ================= CONFIG NULL =================

    @Test
    void configNull_shouldThrow() {

        EventEnvelope e = env(META_INSERT);

        when(loader.get("PAYMENT")).thenReturn(null);

        assertThrows(IngestionService.IngestionProcessingException.class,
                () -> service.ingest(e));
    }
}