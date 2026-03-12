package com.poc.CanonicalIngestionEngine.global;

import com.poc.CanonicalIngestionEngine.service.IngestionService.IngestionProcessingException;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.*;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void shouldHandleIngestionProcessingException() {

        IngestionProcessingException ex =
                new IngestionProcessingException("Failed to process", new RuntimeException());

        ResponseEntity<?> response = handler.handleIngestion(ex);

        assertEquals(500, response.getStatusCode().value());
        assertNotNull(response.getBody());
    }

    @Test
    void shouldHandleGenericException() {

        Exception ex = new RuntimeException("Some error");

        ResponseEntity<?> response = handler.handleGeneral(ex);

        assertEquals(500, response.getStatusCode().value());
        assertNotNull(response.getBody());
    }
}