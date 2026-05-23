package com.poc.CanonicalIngestionEngine.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.CanonicalIngestionEngine.model.EventEnvelope;
import com.poc.CanonicalIngestionEngine.model.IngestionResponse;
import com.poc.CanonicalIngestionEngine.service.IngestionService;
import com.poc.CanonicalIngestionEngine.testutil.DbUtils;

import jakarta.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingestion")
public class IngestController {

    private static final Logger log =
            LoggerFactory.getLogger(IngestController.class);

    private final IngestionService ingestionService;

    public IngestController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping
    public ResponseEntity<IngestionResponse> ingest(
            @Valid @RequestBody EventEnvelope envelope) {

        MDC.put("correlationId", envelope.getCorrelationId());

        try {

            log.info(
                    "Received ingestion request | eventId={} | eventName={}",
                    envelope.getEventId(),
                    envelope.getEventName()
            );

            // =====================================================
            // PROCESS EVENT
            // =====================================================

            ingestionService.ingest(envelope);

            log.info(
                    "Ingestion successful | eventId={}",
                    envelope.getEventId()
            );

            // =====================================================
            // READ eventPayload
            // =====================================================

            ObjectMapper mapper = new ObjectMapper();

            JsonNode payload =
                    mapper.readTree(envelope.getEventPayload());

            // =====================================================
            // DYNAMIC TRANSACTION ID
            // =====================================================

            String tranId = null;

            if (payload.has("accountInformationId")) {

                tranId =
                        payload.get("accountInformationId")
                                .asText();

            } else if (payload.has("paymentTransactionId")) {

                tranId =
                        payload.get("paymentTransactionId")
                                .asText();

            }
            else if (payload.has("transactionId")) {

                tranId =
                        payload.get("transactionId")
                                .asText();

            }
            else if (payload.has("authorizationId")) {

                tranId =
                        payload.get("authorizationId")
                                .asText();

            }
            else if (payload.has("fundingTransactionId")) {

                tranId =
                        payload.get("fundingTransactionId")
                                .asText();

            }
            else if (payload.has("avsTranId")) {

                tranId =
                        payload.get("avsTranId")
                                .asText();

            }
            else if (payload.has("nvsTranId")) {

                tranId =
                        payload.get("nvsTranId")
                                .asText();

            }
            else if (payload.has("refundTransactionId")) {

                tranId =
                        payload.get("refundTransactionId")
                                .asText();

            }
            else if (payload.has("reversalTransactionId")) {

                tranId =
                        payload.get("reversalTransactionId")
                                .asText();

            }
            else if (payload.has("settlementTransactionId")) {

                tranId =
                        payload.get("settlementTransactionId")
                                .asText();

            }
            else {

                throw new RuntimeException(
                        "Transaction ID not found in payload"
                );
            }


            // =====================================================
            // FETCH DB DATA
            // =====================================================

            DbUtils dbUtils = new DbUtils();

            Map<String, Object> dbData =
                    new HashMap<>();

            dbData.put(
                    "SEND_TRANSACTIONS",
                    dbUtils.getTransaction(tranId)
            );

            dbData.put(
                    "SEND_TRAN_DTL",
                    dbUtils.getTransactionDetails(tranId)
            );

            dbData.put(
                    "SEND_RECIP_DTL",
                    dbUtils.getRecipientDetails(tranId)
            );

            dbData.put(
                    "SEND_TRAN_ADDR_DTL",
                    dbUtils.getAddressDetails(tranId)
            );

            // =====================================================
            // FINAL RESPONSE
            // =====================================================

            IngestionResponse response =
                    new IngestionResponse(
                            "SUCCESS",
                            envelope.getEventId(),
                            "Event processed successfully",
                            dbData
                    );

            return ResponseEntity
                    .accepted()
                    .body(response);

        } catch (Exception e) {

            log.error(
                    "Error processing ingestion request",
                    e
            );

            IngestionResponse errorResponse =
                    new IngestionResponse(
                            "FAILED",
                            envelope.getEventId(),
                            e.getMessage(),
                            null
                    );

            return ResponseEntity
                    .internalServerError()
                    .body(errorResponse);

        } finally {

            MDC.clear();
        }
    }
}

