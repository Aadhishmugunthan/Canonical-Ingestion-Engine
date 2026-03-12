//package com.poc.CanonicalIngestionEngine.listener;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
//import com.poc.CanonicalIngestionEngine.service.IngestionService;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.slf4j.MDC;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Component;
//
//@Component
//public class CanonicalEventListener {
//
//    private static final Logger log =
//            LoggerFactory.getLogger(CanonicalEventListener.class);
//
//    private final ObjectMapper objectMapper;
//    private final IngestionService ingestionService;
//
//    public CanonicalEventListener(ObjectMapper objectMapper,
//                                  IngestionService ingestionService) {
//        this.objectMapper = objectMapper;
//        this.ingestionService = ingestionService;
//    }
//
//    @KafkaListener(topics = "canonical-topic", groupId = "canonical-group")
//    public void listen(String eventJson) {
//
//        try {
//            // Convert JSON → DTO
//            EventEnvelope envelope =
//                    objectMapper.readValue(eventJson, EventEnvelope.class);
//
//            // Add correlationId to MDC
//            MDC.put("correlationId", envelope.getCorrelationId());
//
//            log.info("Kafka event received | eventId={} | correlationId={}",
//                    envelope.getEventId(),
//                    envelope.getCorrelationId());
//
//            ingestionService.ingest(envelope);
//
//        } catch (Exception e) {
//            log.error("Kafka processing failed", e);
//            // later → send to DLQ here
//        } finally {
//            MDC.clear();
//        }
//    }
//}