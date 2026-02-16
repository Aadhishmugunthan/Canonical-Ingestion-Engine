package com.poc.CanonicalIngestionEngine.config;

import java.util.List;
import java.util.Map;

/**
 * Represents configuration for a single database table.
 * Contains mapping, mandatory fields, and metadata all in one place.
 */
public class TableConfig {

    private String tableName;           // e.g., "SEND_TRANSACTIONS"
    private int order;                  // Processing order
    private String type;                // main, detail, recipient, address
    private String parentIdField;       // e.g., "TRAN_ID" for child tables
    private boolean autoGenerateId;     // Whether to add UUID for ID column

    // Mapping: column_name -> jsonPath
    private Map<String, String> mapping;

    // List of mandatory columns
    private List<String> mandatory;

    // For address tables only
    private List<AddressTypeMapping> addressTypes;

    // Getters and Setters

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getParentIdField() {
        return parentIdField;
    }

    public void setParentIdField(String parentIdField) {
        this.parentIdField = parentIdField;
    }

    public boolean isAutoGenerateId() {
        return autoGenerateId;
    }

    public void setAutoGenerateId(boolean autoGenerateId) {
        this.autoGenerateId = autoGenerateId;
    }

    public Map<String, String> getMapping() {
        return mapping;
    }

    public void setMapping(Map<String, String> mapping) {
        this.mapping = mapping;
    }

    public List<String> getMandatory() {
        return mandatory;
    }

    public void setMandatory(List<String> mandatory) {
        this.mandatory = mandatory;
    }

    public List<AddressTypeMapping> getAddressTypes() {
        return addressTypes;
    }

    public void setAddressTypes(List<AddressTypeMapping> addressTypes) {
        this.addressTypes = addressTypes;
    }

    /**
     * Inner class for address type configurations
     */
    public static class AddressTypeMapping {
        private String type;              // HOME, BILLING, MAILING
        private String rootPath;          // JSON path to address object
        private Map<String, String> fields;  // column -> jsonPath mapping

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getRootPath() {
            return rootPath;
        }

        public void setRootPath(String rootPath) {
            this.rootPath = rootPath;
        }

        public Map<String, String> getFields() {
            return fields;
        }

        public void setFields(Map<String, String> fields) {
            this.fields = fields;
        }
    }
}