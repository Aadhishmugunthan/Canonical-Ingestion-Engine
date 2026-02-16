package com.poc.CanonicalIngestionEngine.controller;

import com.poc.CanonicalIngestionEngine.service.IngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API endpoint for ingesting events
 *
 * Endpoint: POST /ingest
 */
@RestController
public class IngestController {

    @Autowired
    private IngestionService ingestionService;

    /**
     * Main ingestion endpoint
     *
     * POST http://localhost:8080/ingest
     *
     * Body: Complete event JSON with envelope and payload
     */
    @PostMapping("/ingest")
    public ResponseEntity<?> ingest(@RequestBody String eventJson) {
        try {
            ingestionService.ingest(eventJson);

            return ResponseEntity.ok(Map.of(
                    "status", "SUCCESS",
                    "message", "Event ingested successfully"
            ));

        } catch (Exception e) {
            e.printStackTrace();

            return ResponseEntity.internalServerError().body(Map.of(
                    "status", "FAILED",
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Health check endpoint
     *
     * GET http://localhost:8080/health
     */
    @GetMapping("/health")
    public ResponseEntity<?> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Canonical Ingestion Engine"
        ));
    }
}
