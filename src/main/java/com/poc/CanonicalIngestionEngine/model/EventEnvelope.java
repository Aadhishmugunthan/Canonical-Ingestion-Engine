package com.poc.CanonicalIngestionEngine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * Represents the event envelope containing metadata and payload
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventEnvelope {

    @NotBlank(message = "regulatoryRegion is mandatory")
    private String regulatoryRegion;

    @NotBlank(message = "eventSource is mandatory")
    private String eventSource;

    @NotBlank(message = "eventName is mandatory")
    private String eventName;

    @NotBlank(message = "eventId is mandatory")
    private String eventId;

    @NotBlank(message = "correlationId is mandatory")
    private String correlationId;

    @Positive(message = "eventTimestamp must be positive")
    private long eventTimestamp;

    @NotBlank(message = "eventMetadata is mandatory")
    private String eventMetadata;

    @NotBlank(message = "eventPayload is mandatory")
    private String eventPayload;

    private boolean ignore;  // Set internally by RuleEngine

    // Getters and Setters

    public String getRegulatoryRegion() { return regulatoryRegion; }
    public void setRegulatoryRegion(String regulatoryRegion) { this.regulatoryRegion = regulatoryRegion; }

    public String getEventSource() { return eventSource; }
    public void setEventSource(String eventSource) { this.eventSource = eventSource; }

    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public long getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public String getEventMetadata() { return eventMetadata; }
    public void setEventMetadata(String eventMetadata) { this.eventMetadata = eventMetadata; }

    public String getEventPayload() { return eventPayload; }
    public void setEventPayload(String eventPayload) { this.eventPayload = eventPayload; }

    public boolean isIgnore() { return ignore; }
    public void setIgnore(boolean ignore) { this.ignore = ignore; }
}