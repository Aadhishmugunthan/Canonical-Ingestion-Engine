package com.poc.CanonicalIngestionEngine.mapping;

import java.util.Map;

public class AddressRule {
    private String type;
    private String rootPath;
    private Map<String,String> fields;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getRootPath() { return rootPath; }
    public void setRootPath(String rootPath) { this.rootPath = rootPath; }

    public Map<String,String> getFields() { return fields; }
    public void setFields(Map<String,String> fields) { this.fields = fields; }
}