package com.poc.CanonicalIngestionEngine.rules;

import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Business Rules Engine using Easy Rules
 *
 * Evaluates rules defined in YAML files for each event type.
 * Rules can mark events to be ignored based on various conditions.
 */
@Component
public class RuleEngine {

    @Autowired
    private RulesEngine rulesEngine;

    @Autowired
    private RuleLoader ruleLoader;

    /**
     * Apply business rules to an event envelope
     *
     * @param envelope - Event to evaluate
     */
    public void apply(EventEnvelope envelope) {

        System.out.println("   🔍 Evaluating business rules for: " + envelope.getEventName());

        // Get rules for this event type
        Rules rules = ruleLoader.getRules(envelope.getEventName());

        if (rules.isEmpty()) {
            System.out.println("   ℹ️  No rules defined for " + envelope.getEventName() + ", proceeding...");
            envelope.setIgnore(false);
            return;
        }

        // Create facts (variables available to rules)
        Facts facts = new Facts();
        facts.put("envelope", envelope);
        facts.put("eventName", envelope.getEventName());
        facts.put("eventId", envelope.getEventId());
        facts.put("eventSource", envelope.getEventSource());
        facts.put("eventMetadata", envelope.getEventMetadata());
        facts.put("eventPayload", envelope.getEventPayload());
        facts.put("correlationId", envelope.getCorrelationId());
        facts.put("regulatoryRegion", envelope.getRegulatoryRegion());

        // Fire rules
        rulesEngine.fire(rules, facts);

        // Log result
        if (envelope.isIgnore()) {
            System.out.println("   ❌ Event marked for IGNORE by rules");
        } else {
            System.out.println("   ✅ Event passed all rules");
        }
    }
}