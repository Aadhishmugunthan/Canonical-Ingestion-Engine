package com.poc.CanonicalIngestionEngine.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.mvel.MVELRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RuleLoader {

    private static final Logger log =
            LoggerFactory.getLogger(RuleLoader.class);

    private final Map<String, Rules> rulesCache =
            new ConcurrentHashMap<>();

    private static final List<String> RULE_FILES = List.of(
            "ais-rules",
            "ais2-rules",
            "avs-rules",
            "nvs-rules",
            "payment-rules",
            "funding-rules",
            "auth-rules",
            "clearing-rules",
            "settlement-rules",
            "reversal-rules",
            "refund-rules"
    );

    @Value("${config.server.url:http://localhost:8888}")
    private String configServerUrl;

    @PostConstruct
    public void init() {

        log.info("========================================");
        log.info("RuleLoader: Loading Business Rules");
        log.info("========================================");

        ObjectMapper mapper = new ObjectMapper();

        for (String ruleFile : RULE_FILES) {

            try {

                String url =
                        configServerUrl + "/" + ruleFile + "/default";

                log.info("Fetching rules from: {}", url);

                Map<String, Object> response =
                        mapper.readValue(new URL(url), Map.class);

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> propertySources =
                        (List<Map<String, Object>>)
                                response.get("propertySources");

                if (propertySources == null ||
                        propertySources.isEmpty()) {

                    log.warn(
                            "No propertySources found for {}",
                            ruleFile
                    );

                    continue;
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> source =
                        (Map<String, Object>)
                                propertySources.get(0).get("source");

                Binder binder = new Binder(
                        new MapConfigurationPropertySource(source)
                );

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> ruleDefs =
                        (List<Map<String, Object>>) (List<?>)
                                binder.bind(
                                                "rules",
                                                Bindable.listOf(Map.class)
                                        )
                                        .orElse(Collections.emptyList());

                Rules rules = buildRules(ruleDefs);

                String eventType =
                        extractEventType(ruleFile);

                rulesCache.put(eventType, rules);

                log.info(
                        "Loaded {} rule(s) for event: {}",
                        ruleDefs.size(),
                        eventType
                );

            } catch (Exception e) {

                log.error(
                        "Could not load rules for: {}",
                        ruleFile,
                        e
                );
            }
        }

        log.info("========================================");
        log.info(
                "Loaded rules for {} event type(s)",
                rulesCache.size()
        );
        log.info("========================================");
    }

    private Rules buildRules(
            List<Map<String, Object>> ruleDefs
    ) {

        Rules rules = new Rules();

        for (Map<String, Object> ruleDef : ruleDefs) {

            String name =
                    (String) ruleDef.get("name");

            String description =
                    (String) ruleDef.get("description");

            Integer priority =
                    (Integer) ruleDef.getOrDefault(
                            "priority",
                            1
                    );

            String condition =
                    (String) ruleDef.get("condition");

            Object actionsObj =
                    ruleDef.get("actions");

            List<String> actions =
                    new ArrayList<>();

            if (actionsObj instanceof List<?>) {

                for (Object a : (List<?>) actionsObj) {
                    actions.add(String.valueOf(a));
                }

            } else if (actionsObj instanceof Map<?, ?>) {

                for (Object a :
                        ((Map<?, ?>) actionsObj).values()) {

                    actions.add(String.valueOf(a));
                }
            }

            if (name == null || condition == null) {

                log.warn(
                        "Skipping invalid rule definition: {}",
                        ruleDef
                );

                continue;
            }

            MVELRule rule = new MVELRule()
                    .name(name)
                    .description(description)
                    .priority(priority)
                    .when(condition);

            for (String action : actions) {
                rule.then(action);
            }

            log.info("Registering rule: {}", name);

            rules.register(rule);
        }

        return rules;
    }

    public Rules getRules(String eventType) {

        if (eventType == null) {
            return new Rules();
        }

        return rulesCache.getOrDefault(
                eventType.toUpperCase(),
                new Rules()
        );
    }

    public Set<String> getLoadedEventTypes() {
        return rulesCache.keySet();
    }

    private String extractEventType(
            String filename
    ) {

        if (filename == null) {
            return "";
        }

        return filename
                .replace("-rules", "")
                .replace("_rules", "")
                .replace(".yml", "")
                .replace(".yaml", "")
                .toUpperCase();
    }
}