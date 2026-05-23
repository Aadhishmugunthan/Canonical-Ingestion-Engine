package com.poc.CanonicalIngestionEngine.service;

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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
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

    private IngestionService service;

    @BeforeEach
    void setup() {

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
    // HELPERS
    // =====================================================

    private EventEnvelope envelope(
            String event,
            String operation,
            String payload
    ) {

        EventEnvelope env = new EventEnvelope();

        env.setEventId(UUID.randomUUID().toString());
        env.setEventName(event);

        env.setEventMetadata(
                "{\"operation\":\"" + operation + "\"}"
        );

        env.setEventPayload(payload);

        return env;
    }

    private EventConfig config() {

        TableConfig table = new TableConfig();

        table.setTableName("SEND_TRANSACTIONS");
        table.setType("main");

        table.setMapping(Map.of(
                "TRAN_ID", "$.transactionId",
                "STATUS", "$.status"
        ));

        EventConfig config = new EventConfig();

        config.setEventName("PAYMENT");
        config.setTables(List.of(table));

        return config;
    }

    // =====================================================
    // INSERT FLOW
    // =====================================================

    @Test
    void insert_success() {

        EventEnvelope env = envelope(
                "PAYMENT",
                "A",
                """
                {
                  "transactionId":"TXN1"
                }
                """
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of(
                        "TRAN_ID", "TXN1"
                )));

        when(repository.exists(any(), any(), any()))
                .thenReturn(false);

        when(sqlBuilder.buildInsertSql(any(), any(), anyBoolean()))
                .thenReturn("SQL");

        service.ingest(env);

        verify(repository).insert(anyString(), any());
    }

    @Test
    void insert_emptyMappedData_skipsInsert() {

        EventEnvelope env = envelope(
                "PAYMENT",
                "A",
                "{\"transactionId\":\"TXN2\"}"
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>());

        service.ingest(env);

        verify(repository, never())
                .insert(any(), any());
    }

    @Test
    void insert_duplicateRecord_merges() {

        EventEnvelope env = envelope(
                "PAYMENT",
                "A",
                "{\"transactionId\":\"TXN3\"}"
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of(
                        "TRAN_ID", "TXN3",
                        "STATUS", "INIT"
                )));

        when(repository.exists(any(), any(), any()))
                .thenReturn(true);

        when(repository.findTransaction(any()))
                .thenReturn(new HashMap<>());

        service.ingest(env);

        verify(repository, never())
                .insert(any(), any());
    }

    // =====================================================
    // RULE ENGINE
    // =====================================================

    @Test
    void ignoredEvent_skipsEverything() {

        EventEnvelope env =
                envelope("PAYMENT", "A", "{}");

        doAnswer(invocation -> {

            EventEnvelope e =
                    invocation.getArgument(0);

            e.setIgnore(true);

            return null;

        }).when(ruleEngine).apply(any());

        service.ingest(env);

        verify(repository, never())
                .insert(any(), any());
    }

    // =====================================================
    // STATUS VALIDATION
    // =====================================================

    @Test
    void statusTransition_blocked() {

        EventEnvelope env = envelope(
                "CLEARING",
                "U",
                """
                {
                  "transactionId":"TXN4",
                  "status":"STARTED"
                }
                """
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(repository.exists(any(), any(), any()))
                .thenReturn(true);

        when(repository.findTransaction(any()))
                .thenReturn(Map.of(
                        "STATUS", "SETTLED"
                ));

        service.ingest(env);

        verify(repository, never())
                .updateStatus(any(), any());
    }

    @Test
    void statusTransition_allowed() {

        EventEnvelope env = envelope(
                "CLEARING",
                "U",
                """
                {
                  "transactionId":"TXN5",
                  "status":"SETTLED"
                }
                """
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(repository.exists(any(), any(), any()))
                .thenReturn(true);

        when(repository.findTransaction(any()))
                .thenReturn(Map.of(
                        "STATUS", "INIT"
                ));

        when(repository.findAllRelatedData(any()))
                .thenReturn(new HashMap<>());

        when(repository.columnExists(any(), any()))
                .thenReturn(true);

        when(dataMapper.map(any(), any(), any(), anyBoolean()))
                .thenReturn(new HashMap<>(Map.of(
                        "STATUS", "SETTLED"
                )));

        service.ingest(env);

        verify(repository)
                .updateStatus("TXN5", "SETTLED");
    }

    @Test
    void statusValidation_repositoryException_defaultsTrue() {

        EventEnvelope env = envelope(
                "CLEARING",
                "U",
                """
                {
                  "transactionId":"TXN6",
                  "status":"SETTLED"
                }
                """
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(repository.exists(any(), any(), any()))
                .thenThrow(new RuntimeException("DB ERROR"));

        assertThrows(
                IngestionService.IngestionProcessingException.class,
                () -> service.ingest(env)
        );
    }

    // =====================================================
    // AIS2
    // =====================================================

    @Test
    void ais2_updatesSwSerNum() {

        EventEnvelope env = envelope(
                "AIS2",
                "U",
                """
                {
                  "accountInformationId":"TXN7",
                  "switchSerialNumber":"999"
                }
                """
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(repository.exists(any(), any(), any()))
                .thenReturn(true);

        Map<String, Object> db = new HashMap<>();

        db.put("SW_SER_NUM", null);

        when(repository.findTransaction(any()))
                .thenReturn(db);

        service.ingest(env);

        verify(repository)
                .updateColumn(
                        eq("SEND_TRANSACTIONS"),
                        eq("TRAN_ID"),
                        eq("TXN7"),
                        eq("SW_SER_NUM"),
                        eq("999")
                );
    }

    @Test
    void ais2_skipsWhenAlreadyPopulated() {

        EventEnvelope env = envelope(
                "AIS2",
                "U",
                """
                {
                  "accountInformationId":"TXN8",
                  "switchSerialNumber":"999"
                }
                """
        );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        when(repository.exists(any(), any(), any()))
                .thenReturn(true);

        when(repository.findTransaction(any()))
                .thenReturn(Map.of(
                        "SW_SER_NUM", "111"
                ));

        service.ingest(env);

        verify(repository, never())
                .updateColumn(any(), any(), any(), any(), any());
    }

    // =====================================================
    // MERGE NULL FIELDS
    // =====================================================

    @Test
    void merge_unknownColumn_skipped() throws Exception {

        Method method =
                IngestionService.class.getDeclaredMethod(
                        "mergeNullFields",
                        String.class,
                        String.class,
                        Map.class
                );

        method.setAccessible(true);

        Map<String, Object> incoming =
                new HashMap<>();

        incoming.put("BAD_COLUMN", "X");

        when(repository.findTransaction(any()))
                .thenReturn(Map.of());

        when(repository.columnExists(any(), any()))
                .thenReturn(false);

        method.invoke(
                service,
                "SEND_TRANSACTIONS",
                "TXN9",
                incoming
        );

        verify(repository, never())
                .updateColumn(any(), any(), any(), any(), any());
    }

    // =====================================================
    // TYPE VALIDATION
    // =====================================================

    @Test
    void invalidNonFinTxn_defaultsZero() throws Exception {

        Method method =
                IngestionService.class.getDeclaredMethod(
                        "validateAndConvertTypes",
                        Map.class
                );

        method.setAccessible(true);

        Map<String, Object> map =
                new HashMap<>();

        map.put("NON_FIN_TXN", "ABC");

        method.invoke(service, map);

        assertEquals(0, map.get("NON_FIN_TXN"));
    }

    @Test
    void invalidTranAmt_removed() throws Exception {

        Method method =
                IngestionService.class.getDeclaredMethod(
                        "validateAndConvertTypes",
                        Map.class
                );

        method.setAccessible(true);

        Map<String, Object> map =
                new HashMap<>();

        map.put("TRAN_AMT", "ABC");

        method.invoke(service, map);

        assertFalse(map.containsKey("TRAN_AMT"));
    }

    @Test
    void negativeAmount_throws() throws Exception {

        Method method =
                IngestionService.class.getDeclaredMethod(
                        "validateAndConvertTypes",
                        Map.class
                );

        method.setAccessible(true);

        Map<String, Object> map =
                new HashMap<>();

        map.put("TRAN_AMT", "-100");

        assertThrows(
                Exception.class,
                () -> method.invoke(service, map)
        );
    }

    // =====================================================
    // DEFAULTS
    // =====================================================

    @Test
    void applyDefaults_setsDefaults() throws Exception {

        Method method =
                IngestionService.class.getDeclaredMethod(
                        "applyDefaults",
                        Map.class
                );

        method.setAccessible(true);

        Map<String, Object> map =
                new HashMap<>();

        map.put("TRAN_CRTE_DT", "2025");

        method.invoke(service, map);

        assertEquals("00", map.get("CUR_STAT"));
        assertEquals(0, map.get("NON_FIN_TXN"));
        assertEquals("2025",
                map.get("RPLCTN_UPDT_TS"));
    }

    // =====================================================
    // HELPERS
    // =====================================================

    @Test
    void invalidJson_throwsException() {

        EventEnvelope env =
                envelope(
                        "PAYMENT",
                        "A",
                        "{invalid}"
                );

        when(eventConfigLoader.get(any()))
                .thenReturn(config());

        assertThrows(
                IngestionService.IngestionProcessingException.class,
                () -> service.ingest(env)
        );
    }

    @Test
    void missingConfig_throwsException() {

        EventEnvelope env =
                envelope("UNKNOWN", "A", "{}");

        when(eventConfigLoader.get(any()))
                .thenReturn(null);

        assertThrows(
                IngestionService.IngestionProcessingException.class,
                () -> service.ingest(env)
        );
    }

    @Test
    void invalidMetadata_returnsFalseUpdateOperation() throws Exception {

        Method method =
                IngestionService.class.getDeclaredMethod(
                        "isUpdateOperation",
                        EventEnvelope.class
                );

        method.setAccessible(true);

        EventEnvelope env =
                new EventEnvelope();

        env.setEventMetadata("{bad-json}");

        boolean result =
                (boolean) method.invoke(service, env);

        assertFalse(result);
    }
}