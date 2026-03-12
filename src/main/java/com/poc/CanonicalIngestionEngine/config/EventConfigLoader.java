package com.poc.CanonicalIngestionEngine.config;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URL;
import java.util.*;

@Component
public class EventConfigLoader {

    private final Map<String, EventConfig> configCache = new HashMap<>();

    private static final List<String> EVENT_FILES = List.of(
            "AIS_event",
            "AVS_event",
            "NVS_event",
            "PAYMENT_event",
            "FUNDING_event",
            "AUTH_event",
            "CLEARING_event"
    );

    private static final String CONFIG_SERVER_URL = "http://localhost:8888/";

    @PostConstruct
    public void init() {

        System.out.println("\n========================================");
        System.out.println("🚀 EventConfigLoader: Loading Event Configurations");
        System.out.println("========================================\n");

        ObjectMapper mapper = new ObjectMapper();

        for (String eventFile : EVENT_FILES) {

            String url = CONFIG_SERVER_URL + eventFile + "/default";
            System.out.println("📡 Fetching from Config Server: " + url);

            try {

                Map<String, Object> response =
                        mapper.readValue(new URL(url), Map.class);

                List<Map<String, Object>> propertySources =
                        (List<Map<String, Object>>) response.get("propertySources");

                if (propertySources == null || propertySources.isEmpty()) {
                    System.out.println("⚠️ No property sources found for: " + eventFile);
                    continue;
                }

                Map<String, Object> source =
                        (Map<String, Object>) propertySources.get(0).get("source");

                Binder binder = new Binder(
                        new MapConfigurationPropertySource(source)
                );

                EventConfig config = binder.bind(
                        "",
                        Bindable.of(EventConfig.class)
                ).orElse(null);

                if (config == null) {
                    System.out.println("⚠️ Failed to bind config for: " + eventFile);
                    continue;
                }

                if (config.getTables() != null) {
                    config.getTables()
                            .sort(Comparator.comparingInt(TableConfig::getOrder));
                }

                configCache.put(config.getEventName().toUpperCase(), config);

                System.out.println("✅ Loaded Event: " + config.getEventName());

            } catch (Exception ex) {

                System.out.println("❌ Failed to load config for: " + eventFile);
                System.out.println("Reason: " + ex.getMessage());

            }
        }

        System.out.println("\n========================================");
        System.out.println("✅ Loaded " + configCache.size() + " event configuration(s)");
        System.out.println("========================================\n");
    }

    public EventConfig get(String eventName) {
        if (eventName == null) return null;
        return configCache.get(eventName.toUpperCase());
    }

    public Collection<EventConfig> getAllConfigs() {
        return configCache.values();
    }

    public boolean contains(String eventName) {
        if (eventName == null) return false;
        return configCache.containsKey(eventName.toUpperCase());
    }
}