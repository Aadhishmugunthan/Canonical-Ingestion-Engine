package com.poc.CanonicalIngestionEngine.rules;

import org.jeasy.rules.api.Rules;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RuleLoaderTest {

    private RuleLoader loader;

    @BeforeEach
    void setUp() {
        loader = new RuleLoader();
    }

    // ─────────────────────────────────────────────
    // Constructor
    // ─────────────────────────────────────────────

    @Test
    void testConstructor() {
        assertNotNull(loader);
    }

    // ─────────────────────────────────────────────
    // init() — covers catch branch (no config server)
    // ─────────────────────────────────────────────

    @Test
    void testInitWithNoConfigServer() {
        loader.init();
        assertNotNull(loader.getLoadedEventTypes());
    }

    // ─────────────────────────────────────────────
    // buildRules()
    // ─────────────────────────────────────────────

    @Test
    void testBuildRules_validRuleWithActions() {

        Map<String, Object> def = new HashMap<>();
        def.put("name", "ValidRule");
        def.put("description", "Test rule");
        def.put("priority", 2);
        def.put("condition", "true");
        def.put("actions", List.of("System.out.println(\"fired\")"));

        Rules rules = loader.buildRules(List.of(def));

        assertNotNull(rules);
        assertEquals(1, rules.size());
    }

    @Test
    void testBuildRules_nullActionsStillRegistersRule() {

        Map<String, Object> def = new HashMap<>();
        def.put("name", "NoActionsRule");
        def.put("condition", "true");

        Rules rules = loader.buildRules(List.of(def));

        assertEquals(1, rules.size());
    }

    @Test
    void testBuildRules_invalidRulesSkipped() {

        Map<String, Object> noName = new HashMap<>();
        noName.put("condition", "true");

        Map<String, Object> noCondition = new HashMap<>();
        noCondition.put("name", "BadRule");

        Rules rules = loader.buildRules(List.of(noName, noCondition));

        assertEquals(0, rules.size());
    }

    @Test
    void testBuildRules_defaultPriority() {

        Map<String, Object> def = new HashMap<>();
        def.put("name", "PriorityDefaultRule");
        def.put("condition", "true");

        Rules rules = loader.buildRules(List.of(def));

        assertEquals(1, rules.size());
    }

    @Test
    void testBuildRules_emptyList() {
        Rules rules = loader.buildRules(Collections.emptyList());
        assertTrue(rules.isEmpty());
    }

    @Test
    void testBuildRules_mixOfValidAndInvalid() {

        Map<String, Object> valid = new HashMap<>();
        valid.put("name", "GoodRule");
        valid.put("condition", "true");
        valid.put("actions", List.of("System.out.println(\"ok\")"));

        Map<String, Object> invalid = new HashMap<>();
        invalid.put("condition", "true");

        Rules rules = loader.buildRules(List.of(valid, invalid));

        assertEquals(1, rules.size());
    }

    // ─────────────────────────────────────────────
    // extractEventType() — parameterized test
    // ─────────────────────────────────────────────

    @ParameterizedTest
    @CsvSource({
            "avs-rules, AVS",
            "avs_rules, AVS",
            "avs-rules.yml, AVS",
            "avs_rules.yaml, AVS",
            "ais-rules, AIS",
            "nvs-rules, NVS",
            "payment-rules, PAYMENT",
            "funding-rules, FUNDING"
    })
    void testExtractEventType_variations(String input, String expected) {

        assertEquals(expected, loader.extractEventType(input));
    }

    @Test
    void testExtractEventType_nullInput() {
        assertEquals("", loader.extractEventType(null));
    }

    // ─────────────────────────────────────────────
    // getRules()
    // ─────────────────────────────────────────────

    @Test
    void testGetRules_null() {

        Rules rules = loader.getRules(null);

        assertNotNull(rules);
        assertTrue(rules.isEmpty());
    }

    @Test
    void testGetRules_unknownKey() {

        Rules rules = loader.getRules("UNKNOWN");

        assertNotNull(rules);
        assertTrue(rules.isEmpty());
    }

    @Test
    void testGetRules_lowercaseConverted() throws Exception {

        Map<String, Rules> cache = getCache();
        Rules dummy = new Rules();

        cache.put("AVS", dummy);

        assertSame(dummy, loader.getRules("avs"));
    }

    @Test
    void testGetRules_exactMatch() throws Exception {

        Map<String, Rules> cache = getCache();
        Rules expected = new Rules();

        cache.put("PAYMENT", expected);

        assertSame(expected, loader.getRules("PAYMENT"));
    }

    // ─────────────────────────────────────────────
    // getLoadedEventTypes()
    // ─────────────────────────────────────────────

    @Test
    void testGetLoadedEventTypes_empty() {
        assertNotNull(loader.getLoadedEventTypes());
    }

    @Test
    void testGetLoadedEventTypes_afterInsert() throws Exception {

        Map<String, Rules> cache = getCache();

        cache.put("AIS", new Rules());
        cache.put("NVS", new Rules());

        Set<String> types = loader.getLoadedEventTypes();

        assertTrue(types.contains("AIS"));
        assertTrue(types.contains("NVS"));
    }

    // ─────────────────────────────────────────────
    // Integration: buildRules → getRules
    // ─────────────────────────────────────────────

    @Test
    void testBuildRulesAndRetrieve() throws Exception {

        Map<String, Object> def = new HashMap<>();
        def.put("name", "IntegrationRule");
        def.put("condition", "true");
        def.put("actions", List.of("System.out.println(\"integration\")"));

        Rules built = loader.buildRules(List.of(def));

        getCache().put("INTEGRATION", built);

        Rules result = loader.getRules("integration");

        assertEquals(1, result.size());
    }

    // ─────────────────────────────────────────────
    // Helper
    // ─────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, Rules> getCache() throws Exception {

        Field field = RuleLoader.class.getDeclaredField("rulesCache");
        field.setAccessible(true);

        return (Map<String, Rules>) field.get(loader);
    }
}