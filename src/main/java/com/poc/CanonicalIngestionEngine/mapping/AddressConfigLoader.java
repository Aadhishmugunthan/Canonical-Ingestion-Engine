package com.poc.CanonicalIngestionEngine.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Component
public class AddressConfigLoader {

    private final Map<String,AddressConfig> cache = new HashMap<>();

    @PostConstruct
    public void load() throws Exception {

        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        File dir = new ClassPathResource("mapping").getFile();

        for(File f: dir.listFiles()){
            if(!f.getName().endsWith("_ADDR.yml")) continue;

            AddressConfig cfg = om.readValue(f, AddressConfig.class);
            cache.put(cfg.getTxnType(), cfg);
        }
    }

    public AddressConfig get(String txnType){
        return cache.get(txnType);
    }
}
