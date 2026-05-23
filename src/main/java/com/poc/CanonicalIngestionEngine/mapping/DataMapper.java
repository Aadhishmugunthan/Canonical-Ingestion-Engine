package com.poc.CanonicalIngestionEngine.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class DataMapper {

    private static final Logger log =
            LoggerFactory.getLogger(DataMapper.class);

    public Map<String, Object> map(
            JsonNode payload,
            Map<String, String> columnMappings,
            List<String> mandatoryColumns,
            boolean autoGenerateId
    ) {

        log.info("Mapping JSON to columns");

        String jsonString = payload.toString();

        Map<String, Object> result =
                processMappings(jsonString, columnMappings);

        if (autoGenerateId) {
            String generatedId =
                    UUID.randomUUID().toString();

            result.put("ID", generatedId);

            log.debug(
                    "Generated ID: {}",
                    generatedId
            );
        }

        validateMandatoryFields(
                result,
                mandatoryColumns
        );

        log.info(
                "Mapped {} columns",
                result.size()
        );

        return result;
    }

    public Map<String, Object> mapAddress(
            JsonNode payload,
            String rootPath,
            Map<String, String> fieldMappings,
            String addressType,
            String parentId
    ) {

        log.info(
                "Mapping address type={}",
                addressType
        );

        String jsonString = payload.toString();

        if (!pathExists(jsonString, rootPath)) {

            log.warn(
                    "Address data not found at path={}",
                    rootPath
            );

            return null;
        }

        Map<String, Object> result =
                processMappings(jsonString, fieldMappings);

        result.put(
                "ID",
                UUID.randomUUID().toString()
        );

        result.put(
                "ADDR_TYPE",
                addressType
        );

        if (parentId != null) {
            result.put(
                    "PARENT_ID",
                    parentId
            );
        }

        log.info(
                "Mapped address with {} fields",
                result.size()
        );

        return result;
    }

    // =====================================================
    // COMMON MAPPING LOGIC
    // =====================================================

    private Map<String, Object> processMappings(
            String jsonString,
            Map<String, String> mappings
    ) {

        Map<String, Object> result =
                new HashMap<>();

        for (Map.Entry<String, String> entry :
                mappings.entrySet()) {

            String columnName =
                    entry.getKey();

            String jsonPath =
                    entry.getValue();

            Object mappedValue =
                    extractValue(
                            jsonString,
                            columnName,
                            jsonPath
                    );

            result.put(
                    columnName,
                    mappedValue
            );
        }

        return result;
    }

    private Object extractValue(
            String jsonString,
            String columnName,
            String jsonPath
    ) {

        try {

            String fullPath =
                    jsonPath.startsWith("$")
                            ? jsonPath
                            : "$." + jsonPath;

            Object value =
                    JsonPath.read(
                            jsonString,
                            fullPath
                    );

            return processValue(
                    columnName,
                    value
            );

        } catch (Exception e) {

            log.debug(
                    "Could not map column={} using jsonPath={}",
                    columnName,
                    jsonPath
            );

            return null;
        }
    }

    private Object processValue(
            String columnName,
            Object value
    ) {

        if (value == null) {
            return null;
        }

        if (value instanceof String strValue) {

            String trimmedValue =
                    strValue.trim();

            if (trimmedValue.isEmpty()) {
                return null;
            }

            return convertDateToStandard(
                    columnName,
                    trimmedValue
            );
        }

        if (value instanceof Number ||
                value instanceof Boolean) {

            return value;
        }

        return value.toString();
    }

    private boolean pathExists(
            String jsonString,
            String rootPath
    ) {

        try {

            Object value =
                    JsonPath.read(
                            jsonString,
                            rootPath
                    );

            return value != null;

        } catch (Exception e) {

            return false;
        }
    }

    // =====================================================
    // DATE NORMALIZATION
    // =====================================================

    private Object convertDateToStandard(
            String columnName,
            String value
    ) {

        boolean isDateOrTs =
                columnName.endsWith("_TS")
                        || columnName.endsWith("_DT")
                        || columnName.contains("DATE")
                        || columnName.endsWith("_DOB");

        if (!isDateOrTs) {
            return value;
        }

        if (value.isBlank()) {
            return null;
        }

        String[] formats = {
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss",
                "dd-MM-yyyy HH:mm:ss",
                "yyyy-MM-dd"
        };

        for (String format : formats) {

            try {

                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern(format);

                if (format.contains("HH")) {

                    LocalDateTime localDateTime =
                            LocalDateTime.parse(
                                    value,
                                    formatter
                            );

                    return Timestamp.valueOf(
                            localDateTime
                    );
                }

                LocalDate localDate =
                        LocalDate.parse(
                                value,
                                formatter
                        );

                return Timestamp.valueOf(
                        localDate.atStartOfDay()
                );

            } catch (Exception ignored) {
            }
        }

        try {

            return Timestamp.from(
                    Instant.parse(value)
            );

        } catch (Exception ignored) {
        }

        throw new RuntimeException(
                "Invalid date format for "
                        + columnName
                        + ": "
                        + value
        );
    }

    // =====================================================
    // VALIDATION
    // =====================================================

    private void validateMandatoryFields(
            Map<String, Object> data,
            List<String> mandatory
    ) {

        if (mandatory == null ||
                mandatory.isEmpty()) {

            return;
        }

        for (String field : mandatory) {

            Object value =
                    data.get(field);

            if (value == null ||
                    (
                            value instanceof String str &&
                                    str.trim().isEmpty()
                    )) {

                log.error(
                        "Mandatory field missing: {}",
                        field
                );

                throw new RuntimeException(
                        "Missing mandatory field: "
                                + field
                );
            }
        }
    }
}