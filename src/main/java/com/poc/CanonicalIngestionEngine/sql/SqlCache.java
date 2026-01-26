package com.poc.CanonicalIngestionEngine.sql;

import com.poc.CanonicalIngestionEngine.config.EventConfig;
import com.poc.CanonicalIngestionEngine.config.EventConfigLoader;
import com.poc.CanonicalIngestionEngine.config.TableConfig;
import com.poc.CanonicalIngestionEngine.mapping.AddressConfig;
import com.poc.CanonicalIngestionEngine.mapping.AddressConfigLoader;
import com.poc.CanonicalIngestionEngine.mapping.CompiledMapping;
import com.poc.CanonicalIngestionEngine.mapping.MappingLoader;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
public class SqlCache {
    @Autowired MappingLoader mappingLoader;
    @Autowired AddressConfigLoader addrLoader;
    @Autowired EventConfigLoader eventConfigLoader;

    private final Map<String,String> cache = new HashMap<>();

    @PostConstruct
    public void init() throws Exception {
        System.out.println("=== SqlCache Initialization ===");

        Thread.sleep(100);

        // AUTO-DISCOVER ALL EVENT CONFIGS - NO HARDCODING!
        for (EventConfig eventConfig : eventConfigLoader.getAllConfigs()) {
            buildSqlForEvent(eventConfig.getEventName());
        }

        System.out.println("=== Loaded " + cache.size() + " SQL statements ===");
    }

    private void buildSqlForEvent(String eventName) throws Exception {
        EventConfig eventConfig = eventConfigLoader.get(eventName);
        if (eventConfig == null) {
            System.err.println("WARNING: No config for " + eventName);
            return;
        }

        System.out.println("Building SQL for: " + eventName);

        for (TableConfig table : eventConfig.getTables()) {
            try {
                if ("address".equals(table.getType())) {
                    loadAddressSQL(table, eventName);
                } else {
                    loadRegularSQL(table);
                }
            } catch (Exception e) {
                System.err.println("ERROR: " + table.getSqlKey() + " - " + e.getMessage());
            }
        }
    }

    private void loadRegularSQL(TableConfig table) throws Exception {
        CompiledMapping mapping = mappingLoader.get(table.getMappingKey());
        if (mapping == null) {
            System.err.println("  WARNING: No mapping for " + table.getMappingKey());
            return;
        }

        String sqlFile = getSqlFilePath(table.getSqlKey());
        String template = readFile(sqlFile);
        String sql = SqlBuilder.build(template, mapping.getPaths().keySet());

        cache.put(table.getSqlKey(), sql);
        System.out.println("  ✓ " + table.getSqlKey() + " (" + mapping.getPaths().size() + " cols)");
    }

    private void loadAddressSQL(TableConfig table, String eventName) throws Exception {
        AddressConfig cfg = addrLoader.get(eventName);
        if (cfg == null) {
            System.err.println("  WARNING: No address config for " + eventName);
            return;
        }

        if (cfg.getAddresses() == null || cfg.getAddresses().isEmpty()) {
            System.err.println("  WARNING: No address rules for " + eventName);
            return;
        }

        Set<String> cols = new HashSet<>(cfg.getAddresses().get(0).getFields().keySet());
        cols.add("ID");
        cols.add(table.getParentIdField());
        cols.add("ADDR_TYPE");

        String sqlFile = getSqlFilePath(table.getSqlKey());
        String template = readFile(sqlFile);
        String sql = SqlBuilder.build(template, cols);

        cache.put(table.getSqlKey(), sql);
        System.out.println("  ✓ " + table.getSqlKey() + " (" + cols.size() + " cols)");
    }

    private String readFile(String path) throws Exception {
        return new String(new ClassPathResource(path).getInputStream().readAllBytes());
    }

    private String getSqlFilePath(String sqlKey) {
        Map<String, String> map = new HashMap<>();
        map.put("AVS", "sql/send_transactions.sql");
        map.put("AVS_DTL", "sql/send_tran_dtl.sql");
        map.put("AVS_RECIP", "sql/send_recip_dtl.sql");
        map.put("AVS_ADDR", "sql/send_tran_addr_dtl.sql");

        String path = map.get(sqlKey);
        if (path == null) {
            path = "sql/send_" + sqlKey.toLowerCase() + ".sql";
        }
        return path;
    }

    public String get(String key) {
        String sql = cache.get(key);
        if (sql == null) {
            System.err.println("WARNING: No SQL for " + key);
        }
        return sql;
    }
}