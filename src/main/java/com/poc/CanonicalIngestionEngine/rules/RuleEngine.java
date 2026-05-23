package com.poc.CanonicalIngestionEngine.rules;

import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Business Rules Engine using Easy Rules
 *
 * Evaluates rules defined in YAML files for each event type.
 */
@Component
public class RuleEngine {

    private static final Logger log =
            LoggerFactory.getLogger(RuleEngine.class);

    private final RulesEngine rulesEngine;

    private final RuleLoader ruleLoader;

    public RuleEngine(
            RulesEngine rulesEngine,
            RuleLoader ruleLoader
    ) {
        this.rulesEngine = rulesEngine;
        this.ruleLoader = ruleLoader;
    }

    /**
     * Apply business rules to an event envelope
     */
    public void apply(EventEnvelope envelope) {

        log.info(
                "Evaluating business rules for eventName={} eventId={}",
                envelope.getEventName(),
                envelope.getEventId()
        );

        Rules rules =
                ruleLoader.getRules(envelope.getEventName());

        if (rules.isEmpty()) {

            log.info(
                    "No rules defined for eventName={}",
                    envelope.getEventName()
            );

            envelope.setIgnore(false);

            return;
        }

        Facts facts = new Facts();

        facts.put("envelope", envelope);
        facts.put("eventName", envelope.getEventName());
        facts.put("eventId", envelope.getEventId());
        facts.put("eventSource", envelope.getEventSource());
        facts.put("eventMetadata", envelope.getEventMetadata());
        facts.put("eventPayload", envelope.getEventPayload());
        facts.put("correlationId", envelope.getCorrelationId());
        facts.put("regulatoryRegion", envelope.getRegulatoryRegion());

        rulesEngine.fire(rules, facts);

        if (envelope.isIgnore()) {

            log.warn(
                    "Event marked for IGNORE by rules | eventId={}",
                    envelope.getEventId()
            );

        } else {

            log.info(
                    "Event passed all business rules | eventId={}",
                    envelope.getEventId()
            );
        }
    }
}