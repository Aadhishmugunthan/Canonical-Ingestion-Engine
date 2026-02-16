package com.poc.CanonicalIngestionEngine.model;

/**
 * Represents the event envelope containing metadata and payload
 */
public class EventEnvelope {

    private String regulatoryRegion;
    private String eventSource;
    private String eventName;
    private String eventId;
    private String correlationId;
    private long eventTimestamp;
    private String eventMetadata;
    private String eventPayload;
    private boolean ignore;  // Set by RuleEngine

    // Getters and Setters

    public String getRegulatoryRegion() {
        return regulatoryRegion;
    }

    public void setRegulatoryRegion(String regulatoryRegion) {
        this.regulatoryRegion = regulatoryRegion;
    }

    public String getEventSource() {
        return eventSource;
    }

    public void setEventSource(String eventSource) {
        this.eventSource = eventSource;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getEventMetadata() {
        return eventMetadata;
    }

    public void setEventMetadata(String eventMetadata) {
        this.eventMetadata = eventMetadata;
    }

    public String getEventPayload() {
        return eventPayload;
    }

    public void setEventPayload(String eventPayload) {
        this.eventPayload = eventPayload;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }
}