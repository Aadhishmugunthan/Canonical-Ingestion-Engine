package com.poc.CanonicalIngestionEngine.mapping;

import lombok.Data;

import java.util.Map;

@Data
public class AddressTypeConfig {
    private String type;
    private String root;
    private Map<String,String> mapping;

}

