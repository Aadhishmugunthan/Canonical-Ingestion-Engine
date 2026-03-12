package com.poc.CanonicalIngestionEngine.controller;

import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingestion")
public class IngestController {

    private static final Logger log = LoggerFactory.getLogger(IngestController.class);

    private final IngestionService ingestionService;

    public IngestController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping
    public ResponseEntity<?> ingest(@Valid @RequestBody EventEnvelope envelope) {

        MDC.put("correlationId", envelope.getCorrelationId());

        try {
            log.info("Received ingestion request | eventId={} | eventName={}",
                    envelope.getEventId(),
                    envelope.getEventName());

            ingestionService.ingest(envelope);

            log.info("Ingestion successful | eventId={}", envelope.getEventId());

            return ResponseEntity.accepted().body(
                    Map.of(
                            "status", "SUCCESS",
                            "eventId", envelope.getEventId(),
                            "message", "Event processed successfully"
                    )
            );

        } finally {
            MDC.clear();
        }
    }
}