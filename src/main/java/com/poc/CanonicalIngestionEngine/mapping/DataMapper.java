package com.poc.CanonicalIngestionEngine.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class DataMapper {

    public Map<String, Object> map(JsonNode payload,
                                   Map<String, String> columnMappings,
                                   List<String> mandatoryColumns,
                                   boolean autoGenerateId) {

        Map<String, Object> result = new HashMap<>();
        String jsonString = payload.toString(); // Convert to string for JsonPath

        System.out.println("      🗺️  Mapping JSON to columns...");

        for (Map.Entry<String, String> entry : columnMappings.entrySet()) {
            String columnName = entry.getKey();
            String jsonPath = entry.getValue();

            try {
                Object value = JsonPath.read(jsonString, jsonPath);

                if (value instanceof String) {
                    String strValue = ((String) value).trim();
                    if (strValue.isEmpty()) {
                        result.put(columnName, null);
                    } else {
                        result.put(columnName, convertIfDate(columnName, strValue));
                    }
                } else if (value instanceof Integer || value instanceof Long || value instanceof Double || value instanceof Boolean) {
                    result.put(columnName, value);
                } else {
                    result.put(columnName, value != null ? value.toString() : null);
                }

            } catch (Exception e) {
                result.put(columnName, null);
            }
        }

        if (autoGenerateId) {
            result.put("ID", UUID.randomUUID().toString());
            System.out.println("         ➕ Generated ID: " + result.get("ID"));
        }

        validateMandatoryFields(result, mandatoryColumns);

        System.out.println("         ✅ Mapped " + result.size() + " columns");

        return result;
    }

    private Object convertIfDate(String columnName, String value) {
        if (columnName.endsWith("_DT") || columnName.endsWith("_TS") || columnName.contains("DATE")) {
            try {
                Instant instant = Instant.parse(value);
                return Timestamp.from(instant);
            } catch (Exception e) {
                return value;
            }
        }
        return value;
    }

    private void validateMandatoryFields(Map<String, Object> data, List<String> mandatory) {
        if (mandatory == null || mandatory.isEmpty()) {
            return;
        }

        for (String field : mandatory) {
            Object value = data.get(field);
            if (value == null || (value instanceof String && ((String) value).trim().isEmpty())) {
                throw new RuntimeException("Missing mandatory field: " + field);
            }
        }
    }

    public Map<String, Object> mapAddress(JsonNode payload,
                                          String rootPath,
                                          Map<String, String> fieldMappings,
                                          String addressType,
                                          String parentId) {

        System.out.println("         🏠 Mapping address type: " + addressType);

        String jsonString = payload.toString();

        // Navigate to root path if not "$"
        Object addressData;
        try {
            addressData = JsonPath.read(jsonString, rootPath);
        } catch (Exception e) {
            System.out.println("            ⚠️  Address data not found at: " + rootPath);
            return null;
        }

        if (addressData == null) {
            return null;
        }

        Map<String, Object> result = new HashMap<>();
        String addressJson = addressData.toString();

        for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
            String columnName = entry.getKey();
            String jsonPath = entry.getValue();

            try {
                // For relative paths within the address object
                String fullPath = jsonPath.startsWith("$") ? jsonPath : "$." + jsonPath;
                Object value = JsonPath.read(addressJson, fullPath);

                if (value instanceof String) {
                    String strValue = ((String) value).trim();
                    result.put(columnName, strValue.isEmpty() ? null : strValue);
                } else {
                    result.put(columnName, value != null ? value.toString() : null);
                }
            } catch (Exception e) {
                result.put(columnName, null);
            }
        }

        result.put("ID", UUID.randomUUID().toString());
        result.put("ADDR_TYPE", addressType);

        if (parentId != null) {
            result.put("PARENT_ID", parentId);
        }

        System.out.println("            ✅ Mapped address with " + result.size() + " fields");

        return result;
    }
}