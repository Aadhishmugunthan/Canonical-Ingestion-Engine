package com.poc.CanonicalIngestionEngine.config;

import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Easy Rules Configuration
 *
 * Configures the rules engine for evaluating business rules
 */
@Configuration
public class EasyRulesConfig {

    /**
     * Create a default rules engine bean
     *
     * Configuration:
     * - Skip on first applied rule: false (evaluate all rules)
     * - Skip on first failed rule: false (continue even if rule fails)
     * - Skip on first non-triggered rule: false (evaluate all rules)
     */
    @Bean
    public RulesEngine rulesEngine() {
        return new DefaultRulesEngine();
    }
}






