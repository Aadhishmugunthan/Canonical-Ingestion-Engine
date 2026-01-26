package com.poc.CanonicalIngestionEngine.model;

import lombok.Data;

@Data
public class EventEnvelope {

    private String regulatoryRegion;
    private String eventSource;
    private String eventName;
    private String eventId;
    private String correlationId;
    private long eventTimestamp;
    private String eventMetadata;
    private String eventPayload;
    //  REQUIRED for Drools
    private boolean ignore;
}
