package com.poc.CanonicalIngestionEngine.global;

import com.poc.CanonicalIngestionEngine.service.IngestionService.IngestionProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    // ================= INGESTION ERROR =================

    @ExceptionHandler(IngestionProcessingException.class)
    public ResponseEntity<Map<String, String>> handleIngestion(IngestionProcessingException ex) {

        log.error("Ingestion error occurred", ex);

        Map<String, String> response = new HashMap<>();
        response.put("errorCode", "ING-500");
        response.put("message", ex.getMessage());

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(response);
    }

    // ================= GENERIC ERROR =================

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleGeneral(Exception ex) {

        log.error("Unexpected server error", ex);

        Map<String, String> response = new HashMap<>();
        response.put("errorCode", "GEN-500");
        response.put("message", "Unexpected server error");

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(response);
    }
}