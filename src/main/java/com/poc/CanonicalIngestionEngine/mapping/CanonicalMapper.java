package com.poc.CanonicalIngestionEngine.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.springframework.stereotype.Component;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

@Component
public class CanonicalMapper {

    // Oracle date format: DD-MON-YYYY HH24:MI:SS
    private static final SimpleDateFormat ORACLE_DATE_FORMAT = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

    static {
        ORACLE_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public Map<String,Object> map(JsonNode payload, CompiledMapping cfg){
        Map<String,Object> out = new HashMap<>();
        cfg.getPaths().forEach((k,v)->{
            try{
                Object value = JsonPath.read(payload.toString(), v.getPath());

                // Handle empty strings as null
                if (value instanceof String) {
                    String strValue = ((String) value).trim();
                    if (strValue.isEmpty()) {
                        out.put(k, null);
                    } else {
                        // Convert date fields to Oracle format
                        if (k.endsWith("_DT") || k.contains("_CRTE_") || k.contains("DATE")) {
                            out.put(k, convertToOracleDate(strValue));
                        } else {
                            out.put(k, strValue);
                        }
                    }
                } else {
                    out.put(k, value);
                }
            }catch(Exception e){
                out.put(k,null);
            }
        });

        // Check mandatory fields
        for(String m: cfg.getMandatory()) {
            Object val = out.get(m);
            if(val == null || (val instanceof String && ((String)val).trim().isEmpty())) {
                throw new RuntimeException("Missing mandatory field: " + m);
            }
        }
        return out;
    }

    private String convertToOracleDate(String isoDate) {
        try {
            // Parse ISO-8601 format (e.g., "2026-01-22T10:30:00Z")
            Instant instant = Instant.parse(isoDate);
            // Convert to Oracle format
            return ORACLE_DATE_FORMAT.format(java.util.Date.from(instant));
        } catch (Exception e) {
            // If parsing fails, try as-is or return null
            System.err.println("Failed to parse date: " + isoDate + " - " + e.getMessage());
            return isoDate;
        }
    }
}