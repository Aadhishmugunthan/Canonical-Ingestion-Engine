package com.poc.CanonicalIngestionEngine.controller;

import com.poc.CanonicalIngestionEngine.service.IngestionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/ingest")
public class IngestController {

    private final IngestionService svc;

    public IngestController(IngestionService svc) {
        this.svc = svc;
    }

    @PostMapping
    public ResponseEntity<?> ingest(@RequestBody String json) {
        try {
            svc.ingest(json);
            return ResponseEntity.ok(
                    Map.of("status","SUCCESS","message","Transaction ingested"));
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError()
                    .body(Map.of("status","FAILED","error", e.getMessage()));
        }
    }
}
