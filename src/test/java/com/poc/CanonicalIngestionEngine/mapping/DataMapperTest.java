package com.poc.CanonicalIngestionEngine.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataMapperTest {

    private DataMapper dataMapper;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {

        dataMapper = new DataMapper();
        objectMapper = new ObjectMapper();
    }

    // =====================================================
    // BASIC MAP TESTS
    // =====================================================

    @Test
    void testSimpleMapping() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {
                  "avsTranId":"TXN123",
                  "transactionType":"AVS",
                  "acctNum":"123456"
                }
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("TRAN_TYPE", "$.transactionType");
        mapping.put("ACCT", "$.acctNum");

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                "TXN123",
                result.get("TRAN_ID")
        );

        assertEquals(
                "AVS",
                result.get("TRAN_TYPE")
        );

        assertEquals(
                "123456",
                result.get("ACCT")
        );
    }

    @Test
    void testNullHandling() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {
                  "avsTranId":"TXN1",
                  "transactionType":null,
                  "acctNum":""
                }
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("TYPE", "$.transactionType");
        mapping.put("ACC", "$.acctNum");

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                "TXN1",
                result.get("TRAN_ID")
        );

        assertNull(result.get("TYPE"));
        assertNull(result.get("ACC"));
    }

    @Test
    void testMissingPath() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {
                  "avsTranId":"TXN1"
                }
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put("TRAN_ID", "$.avsTranId");
        mapping.put("MISS", "$.notExist");

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                "TXN1",
                result.get("TRAN_ID")
        );

        assertNull(result.get("MISS"));
    }

    @Test
    void testAutoGenerateId() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"avsTranId":"TXN1"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_ID",
                "$.avsTranId"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        true
                );

        assertNotNull(
                result.get("ID")
        );
    }

    // =====================================================
    // DATE TESTS
    // =====================================================

    @Test
    void testIsoDateConversion() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"createDate":"2024-01-01T10:00:00Z"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_CRTE_DT",
                "$.createDate"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertTrue(
                result.get("TRAN_CRTE_DT")
                        instanceof Timestamp
        );
    }

    @Test
    void testNormalTimestampConversion() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"createDate":"2024-01-01 10:15:30"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_CRTE_TS",
                "$.createDate"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertTrue(
                result.get("TRAN_CRTE_TS")
                        instanceof Timestamp
        );
    }

    @Test
    void testDdMmYyyyTimestampConversion() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"createDate":"01-02-2024 11:30:00"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_CRTE_TS",
                "$.createDate"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertTrue(
                result.get("TRAN_CRTE_TS")
                        instanceof Timestamp
        );
    }

    @Test
    void testSimpleDateConversion() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"dob":"2024-01-01"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "CUSTOMER_DOB",
                "$.dob"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertTrue(
                result.get("CUSTOMER_DOB")
                        instanceof Timestamp
        );
    }

    @Test
    @DisplayName("Invalid date should become null")
    void testInvalidDate() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"createDate":"invalid-date"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_CRTE_DT",
                "$.createDate"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertNull(
                result.get("TRAN_CRTE_DT")
        );
    }

    @Test
    void testBlankDateReturnsNull() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"createDate":" "}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_CRTE_DT",
                "$.createDate"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertNull(
                result.get("TRAN_CRTE_DT")
        );
    }

    // =====================================================
    // NUMBER / BOOLEAN / OBJECT
    // =====================================================

    @Test
    void testNumberMapping() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"amount":500}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_AMT",
                "$.amount"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                500,
                result.get("TRAN_AMT")
        );
    }

    @Test
    void testBooleanMapping() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"active":true}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "ACTIVE_FLG",
                "$.active"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                true,
                result.get("ACTIVE_FLG")
        );
    }

    @Test
    void testObjectToStringBranch() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {
                  "obj":{
                    "a":"b"
                  }
                }
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "OBJ_COL",
                "$.obj"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertNotNull(
                result.get("OBJ_COL")
        );

        assertTrue(
                result.get("OBJ_COL")
                        .toString()
                        .contains("a")
        );
    }

    // =====================================================
    // VALIDATION
    // =====================================================

    @Test
    void testMandatoryMissing() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"transactionType":"AVS"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_ID",
                "$.avsTranId"
        );

        List<String> mandatory =
                List.of("TRAN_ID");

        assertThrows(
                RuntimeException.class,
                () -> dataMapper.map(
                        payload,
                        mapping,
                        mandatory,
                        false
                )
        );
    }

    @Test
    void testMandatoryValidationSuccess() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"avsTranId":"TXN-1"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_ID",
                "$.avsTranId"
        );

        List<String> mandatory =
                List.of("TRAN_ID");

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        mandatory,
                        false
                );

        assertEquals(
                "TXN-1",
                result.get("TRAN_ID")
        );
    }

    @Test
    void testMandatoryEmptyList() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"avsTranId":"TXN-1"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_ID",
                "$.avsTranId"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        List.of(),
                        false
                );

        assertEquals(
                "TXN-1",
                result.get("TRAN_ID")
        );
    }

    // =====================================================
    // NESTED + ARRAY
    // =====================================================

    @Test
    void testNestedJson() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"transaction":{"details":{"id":"TXN9"}}}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_ID",
                "$.transaction.details.id"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                "TXN9",
                result.get("TRAN_ID")
        );
    }

    @Test
    void testArrayPath() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"transactions":[{"id":"T1"}]}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "TRAN_ID",
                "$.transactions[0].id"
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertEquals(
                "T1",
                result.get("TRAN_ID")
        );
    }

    // =====================================================
    // ADDRESS TESTS
    // =====================================================

    @Test
    void testMapAddress() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"street":"123 Main"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "STREET",
                "street"
        );

        Map<String, Object> result =
                dataMapper.mapAddress(
                        payload,
                        "$",
                        mapping,
                        "HOME",
                        "P1"
                );

        assertNotNull(result);

        assertEquals(
                "HOME",
                result.get("ADDR_TYPE")
        );

        assertEquals(
                "P1",
                result.get("PARENT_ID")
        );

        assertNotNull(
                result.get("ID")
        );
    }

    @Test
    void testMapAddress_parentIdNull() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"street":"123 Main"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "STREET",
                "street"
        );

        Map<String, Object> result =
                dataMapper.mapAddress(
                        payload,
                        "$",
                        mapping,
                        "OFFICE",
                        null
                );

        assertNotNull(result);

        assertEquals(
                "OFFICE",
                result.get("ADDR_TYPE")
        );

        assertFalse(
                result.containsKey("PARENT_ID")
        );
    }

    @Test
    void testMapAddress_invalidRoot_returnsNull() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"street":"123 Main"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "STREET",
                "street"
        );

        Map<String, Object> result =
                dataMapper.mapAddress(
                        payload,
                        "$.bad.path",
                        mapping,
                        "HOME",
                        "P1"
                );

        assertNull(result);
    }

    @Test
    void testMapAddress_objectToStringBranch() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {
                  "meta":{
                    "city":"Chennai"
                  }
                }
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "META",
                "meta"
        );

        Map<String, Object> result =
                dataMapper.mapAddress(
                        payload,
                        "$",
                        mapping,
                        "HOME",
                        "P1"
                );

        assertNotNull(result);

        assertTrue(
                result.get("META")
                        .toString()
                        .contains("city")
        );
    }

    // =====================================================
    // EXTRA BRANCH COVERAGE
    // =====================================================

    @Test
    void testNonDollarJsonPath() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"name":"Aadhish"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "NAME",
                "name"
        );

        Map<String, Object> result =
                dataMapper.mapAddress(
                        payload,
                        "$",
                        mapping,
                        "HOME",
                        "P1"
                );

        assertEquals(
                "Aadhish",
                result.get("NAME")
        );
    }

    @Test
    void testInvalidJsonPathCatchBranch() throws Exception {

        JsonNode payload =
                objectMapper.readTree("""
                {"name":"Aadhish"}
                """);

        Map<String, String> mapping =
                new HashMap<>();

        mapping.put(
                "NAME",
                "$.bad["
        );

        Map<String, Object> result =
                dataMapper.map(
                        payload,
                        mapping,
                        null,
                        false
                );

        assertNull(
                result.get("NAME")
        );
    }
}