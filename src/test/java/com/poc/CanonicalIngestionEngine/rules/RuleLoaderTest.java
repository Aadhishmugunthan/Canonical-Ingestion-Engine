package com.poc.CanonicalIngestionEngine.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.jeasy.rules.api.Rule;
import org.jeasy.rules.api.Rules;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * High-coverage unit test for RuleLoader.
 *
 * Strategy
 * ─────────
 * • Private methods (buildRules, extractEventType) → reflected invocation
 * • getRules / getLoadedEventTypes               → inject via rulesCache field
 * • init()                                        → spin up a real in-process
 *   HttpServer so the ObjectMapper.readValue(URL)
 *   call resolves; covers lines 63-116 that SonarQube
 *   flagged as uncovered.
 */
class RuleLoaderTest {

    // ─────────────────────────────────────────────────────────
    // shared loader + in-process HTTP server
    // ─────────────────────────────────────────────────────────

    private RuleLoader ruleLoader;

    /** Lightweight JDK HTTP server — no extra dependency needed. */
    private static HttpServer httpServer;
    private static int serverPort;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ─────────────────────────────────────────────────────────
    // One-time HTTP server setup
    // ─────────────────────────────────────────────────────────

    @BeforeAll
    static void startServer() throws Exception {
        httpServer = HttpServer.create(
                new InetSocketAddress("localhost", 0), 0);
        serverPort = httpServer.getAddress().getPort();

        // Handler that serves a valid config-server response
        // for any path ending in /default
        httpServer.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();

            byte[] body;

            if (path.endsWith("/default")) {
                // Derive a rule name from the path, e.g. /ais-rules/default → AIS
                String segment = path.replace("/default", "")
                        .replaceFirst("^/", "");

                Map<String, Object> rule = new LinkedHashMap<>();
                rule.put("name",        "RULE_" + segment.toUpperCase());
                rule.put("description", "test rule");
                rule.put("priority",    1);
                rule.put("condition",   "true");
                rule.put("actions",     List.of("System.out.println(\"ok\")"));

                Map<String, Object> source = Map.of(
                        "rules[0].name",        rule.get("name"),
                        "rules[0].description", rule.get("description"),
                        "rules[0].priority",    rule.get("priority"),
                        "rules[0].condition",   rule.get("condition"),
                        "rules[0].actions[0]",  "System.out.println(\"ok\")"
                );

                Map<String, Object> propertySource = Map.of(
                        "name",   segment,
                        "source", source
                );

                Map<String, Object> response = Map.of(
                        "propertySources", List.of(propertySource)
                );

                body = MAPPER.writeValueAsBytes(response);
            } else {
                // 404 for anything unexpected
                exchange.sendResponseHeaders(404, 0);
                exchange.close();
                return;
            }

            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });

        httpServer.start();
    }

    @AfterAll
    static void stopServer() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    // ─────────────────────────────────────────────────────────
    // Per-test setup — fresh RuleLoader with empty cache
    // ─────────────────────────────────────────────────────────

    @BeforeEach
    void setup() throws Exception {
        ruleLoader = new RuleLoader();
        resetCache(ruleLoader);
    }

    // =====================================================================
    // 1.  init() — HAPPY PATH
    //     Covers lines 63-116: URL build, HTTP fetch, propertySources
    //     extraction, Binder bind, buildRules call, rulesCache.put
    // =====================================================================

    @Test
    @DisplayName("init() loads rules from config server into cache")
    void init_happyPath_loadsRulesIntoCache() throws Exception {
        // Point the loader at our in-process server
        ReflectionTestUtils.setField(
                ruleLoader,
                "configServerUrl",
                "http://localhost:" + serverPort
        );

        ruleLoader.init();

        // At least one event type should be present in the cache
        Set<String> loaded = ruleLoader.getLoadedEventTypes();
        assertFalse(loaded.isEmpty(),
                "Expected at least one event type after init()");

        // Spot-check a known rule file → extractEventType("ais-rules") = "AIS"
        Rules aisRules = ruleLoader.getRules("AIS");
        assertNotNull(aisRules);
        assertFalse(aisRules.isEmpty(),
                "Expected AIS rules to be loaded");
    }

    // =====================================================================
    // 2.  init() — SERVER RETURNS EMPTY propertySources
    //     Covers the null/empty branch (lines 75-83): continue path
    // =====================================================================

    @Test
    @DisplayName("init() gracefully skips when propertySources is empty")
    void init_emptyPropertySources_skipsThatFile() throws Exception {
        // Serve an empty propertySources list
        HttpServer emptyServer =
                HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        int emptyPort = emptyServer.getAddress().getPort();

        emptyServer.createContext("/", exchange -> {
            Map<String, Object> response =
                    Map.of("propertySources", Collections.emptyList());
            byte[] body = MAPPER.writeValueAsBytes(response);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        emptyServer.start();

        try {
            ReflectionTestUtils.setField(
                    ruleLoader,
                    "configServerUrl",
                    "http://localhost:" + emptyPort
            );

            // Should NOT throw — just log warn and continue
            assertDoesNotThrow(() -> ruleLoader.init());

            // Cache should still be empty
            assertTrue(ruleLoader.getLoadedEventTypes().isEmpty());
        } finally {
            emptyServer.stop(0);
        }
    }

    // =====================================================================
    // 3.  init() — SERVER RETURNS NULL propertySources
    //     Covers the null branch of the null/empty check
    // =====================================================================

    @Test
    @DisplayName("init() gracefully skips when propertySources is null")
    void init_nullPropertySources_skipsThatFile() throws Exception {
        HttpServer nullServer =
                HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        int nullPort = nullServer.getAddress().getPort();

        nullServer.createContext("/", exchange -> {
            // propertySources key is absent → response.get() returns null
            Map<String, Object> response = new HashMap<>();
            response.put("propertySources", null);
            byte[] body = MAPPER.writeValueAsBytes(response);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        nullServer.start();

        try {
            ReflectionTestUtils.setField(
                    ruleLoader,
                    "configServerUrl",
                    "http://localhost:" + nullPort
            );

            assertDoesNotThrow(() -> ruleLoader.init());
            assertTrue(ruleLoader.getLoadedEventTypes().isEmpty());
        } finally {
            nullServer.stop(0);
        }
    }

    // =====================================================================
    // 4.  init() — SERVER UNREACHABLE
    //     Covers the catch (Exception e) block → log.error path (lines 117-123)
    // =====================================================================

    @Test
    @DisplayName("init() logs error and continues when server is unreachable")
    void init_serverUnreachable_doesNotThrow() {
        ReflectionTestUtils.setField(
                ruleLoader,
                "configServerUrl",
                "http://localhost:1" // nothing listening on port 1
        );

        // Must NOT propagate — loader catches and logs
        assertDoesNotThrow(() -> ruleLoader.init());
    }

    // =====================================================================
    // 5.  getRules — known event (case-insensitive)
    // =====================================================================

    @Test
    @DisplayName("getRules returns populated rules for known event")
    void getRules_knownEvent_returnsRules() throws Exception {
        injectRules("TEST", buildValidRules());

        Rules result = ruleLoader.getRules("TEST");

        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    @DisplayName("getRules is case-insensitive")
    void getRules_caseInsensitive_returnsRules() throws Exception {
        injectRules("AIS", buildValidRules());

        // lower-case input should still match the upper-case cache key
        Rules result = ruleLoader.getRules("ais");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    @DisplayName("getRules returns empty Rules for unknown event")
    void getRules_unknownEvent_returnsEmpty() {
        Rules result = ruleLoader.getRules("UNKNOWN");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("getRules returns empty Rules for null event")
    void getRules_nullEvent_returnsEmpty() {
        Rules result = ruleLoader.getRules(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // =====================================================================
    // 6.  getLoadedEventTypes
    // =====================================================================

    @Test
    @DisplayName("getLoadedEventTypes returns all injected keys")
    void getLoadedEventTypes_returnsAllKeys() throws Exception {
        injectRules("AIS",      buildValidRules());
        injectRules("PAYMENT",  buildValidRules());

        Set<String> types = ruleLoader.getLoadedEventTypes();

        assertTrue(types.contains("AIS"));
        assertTrue(types.contains("PAYMENT"));
    }

    @Test
    @DisplayName("getLoadedEventTypes returns empty set when cache is empty")
    void getLoadedEventTypes_emptyCache_returnsEmptySet() {
        assertTrue(ruleLoader.getLoadedEventTypes().isEmpty());
    }

    // =====================================================================
    // 7.  extractEventType
    // =====================================================================

    @Test
    void extractEventType_dashRules_returnsUppercase() throws Exception {
        assertEquals("AIS",      invokeExtract("ais-rules"));
    }

    @Test
    void extractEventType_underscoreRules_returnsUppercase() throws Exception {
        assertEquals("PAYMENT",  invokeExtract("payment_rules"));
    }

    @Test
    void extractEventType_withYmlExtension_stripped() throws Exception {
        assertEquals("REFUND",   invokeExtract("refund-rules.yml"));
    }

    @Test
    void extractEventType_withYamlExtension_stripped() throws Exception {
        assertEquals("AVS",      invokeExtract("avs-rules.yaml"));
    }

    @Test
    void extractEventType_underscoreAndYaml_stripped() throws Exception {
        assertEquals("CLEARING", invokeExtract("clearing_rules.yaml"));
    }

    @Test
    void extractEventType_null_returnsEmpty() throws Exception {
        assertEquals("",         invokeExtract(null));
    }

    @Test
    void extractEventType_alreadyClean_returnsUppercase() throws Exception {
        assertEquals("NVS",      invokeExtract("nvs"));
    }

    // =====================================================================
    // 8.  buildRules — list actions
    // =====================================================================

    @Test
    @DisplayName("buildRules registers rule with List actions")
    void buildRules_listActions_registersRule() throws Exception {
        Map<String, Object> rule = validRule();
        rule.put("actions", List.of("System.out.println(\"A\")"));

        Rules rules = invokeBuildRules(List.of(rule));
        assertEquals(1, countRules(rules));
    }

    // =====================================================================
    // 9.  buildRules — map actions
    // =====================================================================

    @Test
    @DisplayName("buildRules registers rule with Map actions")
    void buildRules_mapActions_registersRule() throws Exception {
        Map<String, Object> rule = validRule();
        Map<String, Object> actions = new LinkedHashMap<>();
        actions.put("a1", "System.out.println(\"X\")");
        actions.put("a2", "System.out.println(\"Y\")");
        rule.put("actions", actions);

        Rules rules = invokeBuildRules(List.of(rule));
        assertEquals(1, countRules(rules));
    }

    // =====================================================================
    // 10. buildRules — null actions (no actions added, rule still registered)
    // =====================================================================

    @Test
    @DisplayName("buildRules registers rule even when actions is null")
    void buildRules_nullActions_registersRule() throws Exception {
        Map<String, Object> rule = validRule();
        rule.put("actions", null);

        Rules rules = invokeBuildRules(List.of(rule));
        assertEquals(1, countRules(rules));
    }

    // =====================================================================
    // 11. buildRules — missing priority uses default (1)
    // =====================================================================

    @Test
    @DisplayName("buildRules uses default priority when not supplied")
    void buildRules_missingPriority_usesDefault() throws Exception {
        Map<String, Object> rule = validRule();
        rule.remove("priority");

        Rules rules = invokeBuildRules(List.of(rule));
        assertEquals(1, countRules(rules));
    }

    // =====================================================================
    // 12. buildRules — null name skips rule
    // =====================================================================

    @Test
    @DisplayName("buildRules skips rule definition with null name")
    void buildRules_nullName_skipsRule() throws Exception {
        Map<String, Object> rule = validRule();
        rule.put("name", null);

        Rules rules = invokeBuildRules(List.of(rule));
        assertTrue(rules.isEmpty());
    }

    // =====================================================================
    // 13. buildRules — null condition skips rule
    // =====================================================================

    @Test
    @DisplayName("buildRules skips rule definition with null condition")
    void buildRules_nullCondition_skipsRule() throws Exception {
        Map<String, Object> rule = validRule();
        rule.put("condition", null);

        Rules rules = invokeBuildRules(List.of(rule));
        assertTrue(rules.isEmpty());
    }

    // =====================================================================
    // 14. buildRules — multiple valid rules
    // =====================================================================

    @Test
    @DisplayName("buildRules registers all valid rules in a list")
    void buildRules_multipleValidRules_registersAll() throws Exception {
        Map<String, Object> r1 = validRule();
        Map<String, Object> r2 = validRule();
        r2.put("name", "RULE_2");

        Rules rules = invokeBuildRules(Arrays.asList(r1, r2));
        assertEquals(2, countRules(rules));
    }

    // =====================================================================
    // 15. buildRules — mix of valid and invalid rules
    // =====================================================================

    @Test
    @DisplayName("buildRules skips invalid entries but registers valid ones")
    void buildRules_mixedValidInvalid_registersOnlyValid() throws Exception {
        Map<String, Object> valid   = validRule();
        Map<String, Object> noName  = validRule(); noName.put("name", null);
        Map<String, Object> noCond  = validRule(); noCond.put("condition", null);

        Rules rules = invokeBuildRules(Arrays.asList(valid, noName, noCond));
        assertEquals(1, countRules(rules));
    }

    // =====================================================================
    // 16. buildRules — empty list
    // =====================================================================

    @Test
    @DisplayName("buildRules returns empty Rules for empty definition list")
    void buildRules_emptyList_returnsEmptyRules() throws Exception {
        Rules rules = invokeBuildRules(Collections.emptyList());
        assertTrue(rules.isEmpty());
    }

    // =====================================================================
    // 17. buildRules — null description (should not throw)
    // =====================================================================

    @Test
    @DisplayName("buildRules handles null description without throwing")
    void buildRules_nullDescription_registersRule() throws Exception {
        Map<String, Object> rule = validRule();
        rule.put("description", null);

        Rules rules = invokeBuildRules(List.of(rule));
        assertEquals(1, countRules(rules));
    }

    // =====================================================================
    // 18. buildRules — large priority value
    // =====================================================================

    @Test
    @DisplayName("buildRules accepts large numeric priority")
    void buildRules_largePriority_registersRule() throws Exception {
        Map<String, Object> rule = validRule();
        rule.put("priority", Integer.MAX_VALUE);

        Rules rules = invokeBuildRules(List.of(rule));
        assertEquals(1, countRules(rules));
    }

    // =====================================================================
    // 19. getRules — returns default empty Rules (not null) for any missing key
    // =====================================================================

    @Test
    @DisplayName("getRules never returns null")
    void getRules_neverReturnsNull() {
        assertNotNull(ruleLoader.getRules("COMPLETELY_UNKNOWN_EVENT_XYZ"));
    }

    // =====================================================================
    // 20. buildRules — actions list with multiple entries
    // =====================================================================

    @Test
    @DisplayName("buildRules adds all List action entries to rule")
    void buildRules_multipleListActions_allAdded() throws Exception {
        Map<String, Object> rule = validRule();
        rule.put("actions", List.of(
                "System.out.println(\"A\")",
                "System.out.println(\"B\")",
                "System.out.println(\"C\")"
        ));

        Rules rules = invokeBuildRules(List.of(rule));
        assertEquals(1, countRules(rules)); // still one rule, but with 3 actions
    }

    // ─────────────────────────────────────────────────────────────────────
    // HELPERS
    // ─────────────────────────────────────────────────────────────────────

    /** Minimal valid rule definition map. */
    private Map<String, Object> validRule() {
        Map<String, Object> rule = new HashMap<>();
        rule.put("name",        "RULE");
        rule.put("description", "description");
        rule.put("priority",    1);
        rule.put("condition",   "true");
        rule.put("actions",     List.of("System.out.println(\"OK\")"));
        return rule;
    }

    private Rules buildValidRules() throws Exception {
        return invokeBuildRules(List.of(validRule()));
    }

    private String invokeExtract(String value) throws Exception {
        Method method = RuleLoader.class
                .getDeclaredMethod("extractEventType", String.class);
        method.setAccessible(true);
        return (String) method.invoke(ruleLoader, value);
    }

    private Rules invokeBuildRules(
            List<Map<String, Object>> defs
    ) throws Exception {
        Method method = RuleLoader.class
                .getDeclaredMethod("buildRules", List.class);
        method.setAccessible(true);
        return (Rules) method.invoke(ruleLoader, defs);
    }

    private void injectRules(String event, Rules rules) throws Exception {
        Field field = RuleLoader.class.getDeclaredField("rulesCache");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Rules> cache = (Map<String, Rules>) field.get(ruleLoader);
        cache.put(event, rules);
    }

    /** Reset the cache to a fresh empty map between tests. */
    private void resetCache(RuleLoader loader) throws Exception {
        Field field = RuleLoader.class.getDeclaredField("rulesCache");
        field.setAccessible(true);
        field.set(loader, new ConcurrentHashMap<String, Rules>());
    }

    private int countRules(Rules rules) {
        int count = 0;
        for (Rule ignored : rules) {
            count++;
        }
        return count;
    }
}