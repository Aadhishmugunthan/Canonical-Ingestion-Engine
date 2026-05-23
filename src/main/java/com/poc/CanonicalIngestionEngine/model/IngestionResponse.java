package com.poc.CanonicalIngestionEngine.model;

import java.util.Map;

public class IngestionResponse {

    private String status;
    private String eventId;
    private String message;

    private Map<String, Object> dbData;

    public IngestionResponse() {
    }

    public IngestionResponse(
            String status,
            String eventId,
            String message,
            Map<String, Object> dbData
    ) {
        this.status = status;
        this.eventId = eventId;
        this.message = message;
        this.dbData = dbData;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, Object> getDbData() {
        return dbData;
    }

    public void setDbData(Map<String, Object> dbData) {
        this.dbData = dbData;
    }
}