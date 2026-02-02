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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * SqlCache - Builds dynamic INSERT SQL at startup
 *
 * ✅ Inserts ONLY mapped + real DB columns
 * ✅ Auto-adds ID for Recipient/Sender/Receiver
 * ✅ Prevents missing parameter errors
 */
@Component
public class SqlCache {

    @Autowired
    private MappingLoader mappingLoader;

    @Autowired
    private AddressConfigLoader addrLoader;

    @Autowired
    private EventConfigLoader eventConfigLoader;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final Map<String, String> cache = new HashMap<>();

    @PostConstruct
    public void init() throws Exception {

        System.out.println("=== SqlCache Initialization ===");

        Thread.sleep(100);

        for (EventConfig cfg : eventConfigLoader.getAllConfigs()) {
            buildSqlForEvent(cfg.getEventName());
        }

        System.out.println("\n=== SqlCache Loaded SQL Keys ===");
        System.out.println(cache.keySet());
    }

    private void buildSqlForEvent(String eventName) throws Exception {

        EventConfig cfg = eventConfigLoader.get(eventName);
        if (cfg == null) return;

        System.out.println("\nBuilding SQL for event: " + eventName);

        for (TableConfig table : cfg.getTables()) {

            if ("address".equalsIgnoreCase(table.getType())) {
                loadAddressSQL(table, eventName);
            } else {
                loadRegularSQL(table);
            }
        }
    }

    /**
     * ✅ Regular Table SQL Builder
     */
    private void loadRegularSQL(TableConfig table) throws Exception {

        CompiledMapping mapping = mappingLoader.get(table.getMappingKey());
        if (mapping == null) return;

        // Step 1: mapping columns
        Set<String> columns = new HashSet<>(mapping.getPaths().keySet());

        // Step 2: Add ID for recipient tables
        if (isRecipientTable(table)) {
            columns.add("ID");
        }

        // Step 3: Filter only columns that exist in DB
        String tableName = extractTableName(table.getSqlKey());
        Set<String> dbCols = getDbColumns(tableName);

        columns.retainAll(dbCols);

        // Step 4: Build SQL
        String template = readFile(getSqlFilePath(table.getSqlKey()));
        String sql = SqlBuilder.build(template, columns);

        cache.put(table.getSqlKey(), sql);

        System.out.println("  ✓ " + table.getSqlKey()
                + " → " + tableName
                + " cols=" + columns.size());
    }

    /**
     * ✅ Address Table Builder
     */
    private void loadAddressSQL(TableConfig table, String eventName) throws Exception {

        AddressConfig cfg = addrLoader.get(eventName);
        if (cfg == null) return;

        Set<String> cols =
                new HashSet<>(cfg.getAddresses().get(0).getFields().keySet());

        cols.add("ID");
        cols.add(table.getParentIdField());
        cols.add("ADDR_TYPE");

        String tableName = extractTableName(table.getSqlKey());
        cols.retainAll(getDbColumns(tableName));

        String template = readFile(getSqlFilePath(table.getSqlKey()));
        String sql = SqlBuilder.build(template, cols);

        cache.put(table.getSqlKey(), sql);

        System.out.println("  ✓ " + table.getSqlKey()
                + " (ADDR) cols=" + cols.size());
    }

    /**
     * ✅ Get DB Columns from Oracle Metadata
     */
    private Set<String> getDbColumns(String tableName) {

        String sql =
                "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?";

        List<String> cols =
                jdbcTemplate.queryForList(sql, String.class, tableName.toUpperCase());

        return new HashSet<>(cols);
    }

    /**
     * Extract actual table name from sqlKey
     */
    private String extractTableName(String sqlKey) {

        if (sqlKey.contains("RECIP")) return "SEND_RECIP_DTL";
        if (sqlKey.contains("ADDR")) return "SEND_TRAN_ADDR_DTL";
        if (sqlKey.contains("DTL")) return "SEND_TRAN_DTL";

        return "SEND_TRANSACTIONS";
    }

    private boolean isRecipientTable(TableConfig table) {

        String key = table.getMappingKey().toUpperCase();

        return key.contains("RECIP")
                || key.contains("SENDER")
                || key.contains("RECEIVER");
    }

    private String readFile(String path) throws Exception {
        return new String(new ClassPathResource(path)
                .getInputStream()
                .readAllBytes());
    }

    private String getSqlFilePath(String sqlKey) {

        Map<String, String> map = new HashMap<>();

        map.put("AVS", "sql/send_transactions.sql");
        map.put("AVS_DTL", "sql/send_tran_dtl.sql");
        map.put("AVS_RECIP", "sql/send_recip_dtl.sql");
        map.put("AVS_ADDR", "sql/send_tran_addr_dtl.sql");

        map.put("AUTH_SENDER", "sql/send_recip_dtl.sql");
        map.put("AUTH_RECEIVER", "sql/send_recip_dtl.sql");

        return map.getOrDefault(sqlKey,
                "sql/send_" + sqlKey.toLowerCase() + ".sql");
    }

    public String get(String key) {
        return cache.get(key);
    }
}
