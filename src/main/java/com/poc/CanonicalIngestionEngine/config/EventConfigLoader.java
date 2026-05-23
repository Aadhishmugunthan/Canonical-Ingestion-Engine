package com.poc.CanonicalIngestionEngine.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.stereotype.Component;

import java.net.URL;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EventConfigLoader {

    private static final Logger log =
            LoggerFactory.getLogger(EventConfigLoader.class);

    private final Map<String, EventConfig> configCache =
            new ConcurrentHashMap<>();

    private static final List<String> EVENT_FILES = List.of(
            "AIS_event",
            "AIS2_event",
            "AVS_event",
            "NVS_event",
            "PAYMENT_event",
            "FUNDING_event",
            "AUTH_event",
            "CLEARING_event",
            "SETTLEMENT_event",
            "REVERSAL_event",
            "REFUND_event",
            "TRANSACTION_event",
            "DISBURSEMENT_event",
            "PAY_event"
    );

    @Value("${config.server.url:http://localhost:8888}")
    private String configServerUrl;

    @PostConstruct
    public void init() {

        log.info("========================================");
        log.info("EventConfigLoader: Loading Event Configurations");
        log.info("========================================");

        ObjectMapper mapper = new ObjectMapper();

        for (String eventFile : EVENT_FILES) {

            String url =
                    configServerUrl + "/" + eventFile + "/default";

            log.info("Fetching config from: {}", url);

            try {

                Map<String, Object> response =
                        mapper.readValue(new URL(url), Map.class);

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> propertySources =
                        (List<Map<String, Object>>)
                                response.get("propertySources");

                if (propertySources == null ||
                        propertySources.isEmpty()) {

                    log.warn(
                            "No property sources found for: {}",
                            eventFile
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

                EventConfig config = binder.bind(
                        "",
                        Bindable.of(EventConfig.class)
                ).orElse(null);

                if (config == null) {

                    log.warn(
                            "Failed to bind config for: {}",
                            eventFile
                    );

                    continue;
                }

                if (config.getTables() != null) {

                    config.getTables().sort(
                            Comparator.comparingInt(
                                    TableConfig::getOrder
                            )
                    );
                }

                configCache.put(
                        config.getEventName().toUpperCase(),
                        config
                );

                log.info(
                        "Loaded Event Configuration: {}",
                        config.getEventName()
                );

            } catch (Exception ex) {

                log.error(
                        "Failed to load config for: {}",
                        eventFile,
                        ex
                );
            }
        }

        log.info("========================================");
        log.info(
                "Loaded {} event configuration(s)",
                configCache.size()
        );
        log.info("========================================");
    }

    public EventConfig get(String eventName) {

        if (eventName == null) {
            return null;
        }

        return configCache.get(
                eventName.toUpperCase()
        );
    }

    public Collection<EventConfig> getAllConfigs() {
        return configCache.values();
    }

    public boolean contains(String eventName) {

        if (eventName == null) {
            return false;
        }

        return configCache.containsKey(
                eventName.toUpperCase()
        );
    }
}