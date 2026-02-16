package com.poc.CanonicalIngestionEngine.config;

import java.util.List;

/**
 * Root configuration for an event type.
 * One EventConfig = One YAML file = One event (e.g., AVS, AUTH, etc.)
 */
public class EventConfig {

    private String eventName;        // e.g., "AVS"
    private String description;      // Human-readable description
    private List<TableConfig> tables;  // All tables for this event

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<TableConfig> getTables() {
        return tables;
    }

    public void setTables(List<TableConfig> tables) {
        this.tables = tables;
    }
}