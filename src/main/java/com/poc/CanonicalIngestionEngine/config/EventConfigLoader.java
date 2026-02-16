package com.poc.CanonicalIngestionEngine.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads all event configuration YAML files at startup.
 * Each YAML file contains complete table mappings for one event type.
 *
 * Example: AVS_event.yml contains all tables for AVS transactions.
 */
@Component
public class EventConfigLoader {

    private final Map<String, EventConfig> configCache = new HashMap<>();

    @PostConstruct
    public void init() throws Exception {

        System.out.println("\n========================================");
        System.out.println("🚀 EventConfigLoader: Loading Event Configurations");
        System.out.println("========================================\n");

        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

        // Load from resources/event-config directory
        File configDir = new ClassPathResource("event-config").getFile();

        if (!configDir.exists() || !configDir.isDirectory()) {
            throw new RuntimeException("event-config directory not found!");
        }

        File[] files = configDir.listFiles();

        if (files == null || files.length == 0) {
            System.out.println("⚠️  No event configuration files found");
            return;
        }

        // Load each *_event.yml file
        for (File file : files) {

            if (!file.getName().endsWith("_event.yml")) {
                System.out.println("⏭️  Skipping: " + file.getName());
                continue;
            }

            System.out.println("📄 Loading: " + file.getName());

            // Parse YAML into EventConfig object
            EventConfig config = yamlMapper.readValue(file, EventConfig.class);

            // Sort tables by order
            config.getTables().sort((a, b) -> Integer.compare(a.getOrder(), b.getOrder()));

            // Cache by event name
            configCache.put(config.getEventName(), config);

            // Print summary
            System.out.println("   ✅ Event: " + config.getEventName());
            System.out.println("   📋 Description: " + config.getDescription());
            System.out.println("   🗂️  Tables: " + config.getTables().size());

            for (TableConfig table : config.getTables()) {
                System.out.println("      └─ " + table.getTableName()
                        + " (type=" + table.getType()
                        + ", columns=" + (table.getMapping() != null ? table.getMapping().size() : 0)
                        + ")");
            }

            System.out.println();
        }

        System.out.println("========================================");
        System.out.println("✅ Loaded " + configCache.size() + " event configuration(s)");
        System.out.println("========================================\n");
    }

    /**
     * Get configuration for a specific event
     */
    public EventConfig get(String eventName) {
        EventConfig config = configCache.get(eventName);

        if (config == null) {
            System.err.println("❌ No configuration found for event: " + eventName);
        }

        return config;
    }

    /**
     * Get all loaded configurations
     */
    public Collection<EventConfig> getAllConfigs() {
        return configCache.values();
    }
}