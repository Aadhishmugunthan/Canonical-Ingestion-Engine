package com.poc.CanonicalIngestionEngine.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Component
public class EventConfigLoader {
    private final Map<String, EventConfig> cache = new HashMap<>();

    @PostConstruct
    public void load() throws Exception {
        System.out.println("=== EventConfigLoader Initialization ===");
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        File dir = new ClassPathResource("event-config").getFile();

        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("ERROR: event-config directory not found!");
            return;
        }

        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            System.err.println("WARNING: No event config files found");
            return;
        }

        for (File f : files) {
            if (!f.getName().endsWith("_event.yml")) {
                System.out.println("Skipping: " + f.getName());
                continue;
            }

            System.out.println("Loading: " + f.getName());
            EventConfig cfg = om.readValue(f, EventConfig.class);

            cfg.getTables().sort((a, b) -> Integer.compare(a.getOrder(), b.getOrder()));

            cache.put(cfg.getEventName(), cfg);
            System.out.println("  ✓ " + cfg.getEventName() + " - " + cfg.getTables().size() + " tables");

            for (TableConfig table : cfg.getTables()) {
                System.out.println("    - " + table.getMappingKey() + " → " + table.getSqlKey());
            }
        }

        System.out.println("=== Loaded " + cache.size() + " events ===");
    }

    public EventConfig get(String eventName) {
        EventConfig config = cache.get(eventName);
        if (config == null) {
            System.err.println("WARNING: No config for: " + eventName);
        }
        return config;
    }

    public java.util.Collection<EventConfig> getAllConfigs() {
        return cache.values();
    }
}