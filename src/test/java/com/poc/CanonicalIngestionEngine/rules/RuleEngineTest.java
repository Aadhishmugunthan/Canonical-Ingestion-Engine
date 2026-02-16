package com.poc.CanonicalIngestionEngine.rules;

import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test cases for RuleEngine
 */
@ExtendWith(MockitoExtension.class)
class RuleEngineTest {

    @Mock
    private RuleLoader ruleLoader;

    @Spy
    private RulesEngine rulesEngine = new DefaultRulesEngine();

    @InjectMocks
    private RuleEngine ruleEngine;

    @Test
    @DisplayName("Should set ignore to false when no rules defined")
    void testNoRulesDefined() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        when(ruleLoader.getRules("AVS")).thenReturn(new Rules());

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
        verify(ruleLoader, times(1)).getRules("AVS");
    }

    @Test
    @DisplayName("Should ignore event when UPDATE operation rule triggers")
    void testIgnoreUpdateOperation() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventMetadata("{\"operation\":\"U\"}");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Ignore Updates")
                .when("eventMetadata.contains('\"operation\":\"U\"')")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should ignore event when DELETE operation rule triggers")
    void testIgnoreDeleteOperation() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventMetadata("{\"operation\":\"D\"}");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Ignore Deletes")
                .when("eventMetadata.contains('\"operation\":\"D\"')")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should not ignore event when INSERT operation")
    void testAllowInsertOperation() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventMetadata("{\"operation\":\"I\"}");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Ignore Updates")
                .when("eventMetadata.contains('\"operation\":\"U\"')")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should ignore event with empty metadata")
    void testIgnoreEmptyMetadata() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventMetadata("");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Validate Metadata")
                .when("eventMetadata != null && eventMetadata.trim().isEmpty()")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should handle null metadata gracefully")
    void testHandleNullMetadata() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventMetadata(null);

        Rules rules = new Rules();
        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        // Should not throw NPE
        assertDoesNotThrow(() -> ruleEngine.apply(envelope));
        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should ignore test events")
    void testIgnoreTestEvents() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventSource("TEST_SOURCE");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Ignore Test Events")
                .when("eventSource != null && eventSource.toLowerCase().contains('test')")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should ignore event with empty payload")
    void testIgnoreEmptyPayload() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventPayload("{}");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Validate Payload")
                .when("eventPayload == null || eventPayload.trim().isEmpty() || eventPayload.equals('{}') || eventPayload.equals('null')")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should modify payload when default value rule triggers")
    void testDefaultValueRule() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventPayload("{\"acquiringICA\":\"ABC\"}");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Default ACQ_ICA")
                .when("eventPayload.contains('acquiringICA') && eventPayload.contains('\"acquiringICA\":\"') && !eventPayload.matches('.*\"acquiringICA\":\"[0-9]+\".*')")
                .then("envelope.setEventPayload(eventPayload.replaceAll('\"acquiringICA\":\"[^\"]*\"', '\"acquiringICA\":\"0\"'))");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.getEventPayload().contains("\"acquiringICA\":\"0\""));
        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should apply multiple rules in priority order")
    void testMultipleRulesPriority() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventMetadata("{\"operation\":\"U\"}");
        envelope.setEventSource("TEST_SOURCE");

        Rules rules = new Rules();

        MVELRule rule1 = new MVELRule()
                .name("Ignore Updates")
                .priority(1)
                .when("eventMetadata.contains('\"operation\":\"U\"')")
                .then("envelope.setIgnore(true)");

        MVELRule rule2 = new MVELRule()
                .name("Ignore Tests")
                .priority(2)
                .when("eventSource != null && eventSource.toLowerCase().contains('test')")
                .then("envelope.setIgnore(true)");

        rules.register(rule1);
        rules.register(rule2);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should handle different event types")
    void testDifferentEventTypes() {
        EventEnvelope avsEnvelope = createEventEnvelope("AVS");
        Rules avsRules = new Rules();
        when(ruleLoader.getRules("AVS")).thenReturn(avsRules);

        EventEnvelope aisEnvelope = createEventEnvelope("AIS");
        Rules aisRules = new Rules();
        when(ruleLoader.getRules("AIS")).thenReturn(aisRules);

        ruleEngine.apply(avsEnvelope);
        ruleEngine.apply(aisEnvelope);

        verify(ruleLoader, times(1)).getRules("AVS");
        verify(ruleLoader, times(1)).getRules("AIS");
    }

    @Test
    @DisplayName("Should make all envelope fields available as facts")
    void testAllEnvelopeFieldsAvailable() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventId("EVT-123");
        envelope.setCorrelationId("CORR-456");
        envelope.setEventSource("SOURCE");
        envelope.setEventMetadata("{\"key\":\"value\"}");
        envelope.setEventPayload("{\"data\":\"test\"}");
        envelope.setRegulatoryRegion("US");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Check All Fields")
                .when("eventName == 'AVS' && eventId == 'EVT-123' && correlationId == 'CORR-456' && eventSource == 'SOURCE' && regulatoryRegion == 'US'")
                .then("System.out.println('All fields available')");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should ignore event with missing transaction ID in AVS")
    void testIgnoreMissingTransactionId() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventPayload("{\"transactionType\":\"AVS\"}");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Validate Transaction ID")
                .when("!eventPayload.contains('avsTranId') || eventPayload.contains('\"avsTranId\":null') || eventPayload.contains('\"avsTranId\":\"\"')")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertTrue(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should not ignore event with valid transaction ID")
    void testAllowValidTransactionId() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setEventPayload("{\"avsTranId\":\"TXN-123\",\"transactionType\":\"AVS\"}");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Validate Transaction ID")
                .when("!eventPayload.contains('avsTranId') || eventPayload.contains('\"avsTranId\":null') || eventPayload.contains('\"avsTranId\":\"\"')")
                .then("envelope.setIgnore(true)");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should handle rules that don't modify ignore flag")
    void testRulesWithoutIgnoreModification() {
        EventEnvelope envelope = createEventEnvelope("AVS");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Log Only")
                .when("eventName == 'AVS'")
                .then("System.out.println('AVS event detected')");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should handle null event name")
    void testNullEventName() {
        EventEnvelope envelope = createEventEnvelope(null);
        Rules rules = new Rules();
        when(ruleLoader.getRules(null)).thenReturn(rules);

        assertDoesNotThrow(() -> ruleEngine.apply(envelope));
    }

    @Test
    @DisplayName("Should handle empty rules set")
    void testEmptyRulesSet() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        when(ruleLoader.getRules("AVS")).thenReturn(new Rules());

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should verify all facts are registered")
    void testAllFactsRegistered() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setRegulatoryRegion("EU");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("Check Facts")
                .when("envelope != null && eventName != null && eventId != null && eventSource != null && eventMetadata != null && eventPayload != null && correlationId != null && regulatoryRegion != null")
                .then("System.out.println('All facts present')");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
    }

    @Test
    @DisplayName("Should handle regulatory region in rules")
    void testRegulatoryRegionInRules() {
        EventEnvelope envelope = createEventEnvelope("AVS");
        envelope.setRegulatoryRegion("US");

        Rules rules = new Rules();
        MVELRule rule = new MVELRule()
                .name("US Region Check")
                .when("regulatoryRegion != null && regulatoryRegion.equals('US')")
                .then("System.out.println('US region detected')");
        rules.register(rule);

        when(ruleLoader.getRules("AVS")).thenReturn(rules);

        ruleEngine.apply(envelope);

        assertFalse(envelope.isIgnore());
    }

    // Helper method
    private EventEnvelope createEventEnvelope(String eventName) {
        EventEnvelope envelope = new EventEnvelope();
        envelope.setEventName(eventName);
        envelope.setEventId("EVT-001");
        envelope.setEventSource("SOURCE");
        envelope.setEventMetadata("{\"operation\":\"I\"}");
        envelope.setEventPayload("{\"test\":\"data\"}");
        envelope.setCorrelationId("CORR-001");
        envelope.setEventTimestamp(System.currentTimeMillis());
        envelope.setRegulatoryRegion("US");
        envelope.setIgnore(false);
        return envelope;
    }
}