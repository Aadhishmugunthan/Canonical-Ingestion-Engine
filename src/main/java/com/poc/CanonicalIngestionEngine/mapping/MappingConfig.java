package com.poc.CanonicalIngestionEngine.mapping;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class MappingConfig {
    private String txnType;
    private Map<String,String> mapping;
    private List<String> mandatory;
}