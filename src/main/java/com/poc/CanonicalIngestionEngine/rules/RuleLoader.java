package com.poc.CanonicalIngestionEngine.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.jeasy.rules.api.Rule;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.mvel.MVELRule;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;

/**
 * Loads Easy Rules from YAML files at startup
 *
 * Rules are organized by event type:
 * - avs-rules.yml
 * - ais-rules.yml
 * - nvs-rules.yml
 */
@Component
public class RuleLoader {

    private final Map<String, Rules> rulesCache = new HashMap<>();

    @PostConstruct
    public void init() throws Exception {

        System.out.println("\n========================================");
        System.out.println("📋 RuleLoader: Loading Business Rules");
        System.out.println("========================================\n");

        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

        // Load rules from resources/rules directory
        File rulesDir = new ClassPathResource("rules").getFile();

        if (!rulesDir.exists() || !rulesDir.isDirectory()) {
            System.out.println("⚠️  Rules directory not found. No rules loaded.");
            return;
        }

        File[] ruleFiles = rulesDir.listFiles((dir, name) -> name.endsWith("-rules.yml"));

        if (ruleFiles == null || ruleFiles.length == 0) {
            System.out.println("⚠️  No rule files found.");
            return;
        }

        // Load each rule file
        for (File ruleFile : ruleFiles) {

            String eventType = extractEventType(ruleFile.getName());

            System.out.println("📄 Loading: " + ruleFile.getName() + " (Event: " + eventType + ")");

            // Parse YAML to list of rule definitions
            List<Map<String, Object>> ruleDefs = yamlMapper.readValue(
                    ruleFile,
                    List.class
            );

            Rules rules = new Rules();

            for (Map<String, Object> ruleDef : ruleDefs) {

                String name = (String) ruleDef.get("name");
                String description = (String) ruleDef.get("description");
                Integer priority = (Integer) ruleDef.getOrDefault("priority", 1);
                String condition = (String) ruleDef.get("condition");
                List<String> actions = (List<String>) ruleDef.get("actions");

                // Build MVEL rule
                MVELRule rule = new MVELRule()
                        .name(name)
                        .description(description)
                        .priority(priority)
                        .when(condition);

                // Add actions
                for (String action : actions) {
                    rule.then(action);
                }

                rules.register(rule);

                System.out.println("   ✅ " + name + " (priority=" + priority + ")");
            }

            rulesCache.put(eventType, rules);
            System.out.println("   📊 Loaded " + rules.size() + " rule(s) for " + eventType);
            System.out.println();
        }

        System.out.println("========================================");
        System.out.println("✅ Loaded rules for " + rulesCache.size() + " event type(s)");
        System.out.println("========================================\n");
    }

    /**
     * Get rules for a specific event type
     *
     * @param eventType - Event type (AVS, AIS, NVS)
     * @return Rules object, or empty Rules if not found
     */
    public Rules getRules(String eventType) {

        if (eventType == null) {
            return new Rules();
        }

        Rules rules = rulesCache.get(eventType.toUpperCase());

        if (rules == null) {
            System.out.println("⚠️  No rules found for event type: " + eventType);
            return new Rules();
        }

        return rules;
    }

    /**
     * Extract event type from filename
     *
     * Example: "avs-rules.yml" → "AVS"
     */
    private String extractEventType(String filename) {
        return filename
                .replace("-rules.yml", "")
                .toUpperCase();
    }

    /**
     * Get all loaded event types
     */
    public Set<String> getLoadedEventTypes() {
        return rulesCache.keySet();
    }
}