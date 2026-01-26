package com.poc.CanonicalIngestionEngine.rules;

import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.springframework.stereotype.Component;

@Component
public class RuleEngine {

    private final KieContainer container;

    public RuleEngine() {
        this.container = KieServices.Factory.get()
                .getKieClasspathContainer();
    }

    public void apply(EventEnvelope event) {
        KieSession session = container.newKieSession();
        try {
            session.insert(event);
            session.fireAllRules();
        } finally {
            session.dispose();
        }
    }
}
