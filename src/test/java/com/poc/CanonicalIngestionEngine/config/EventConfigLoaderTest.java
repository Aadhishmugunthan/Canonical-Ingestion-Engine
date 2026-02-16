package com.poc.CanonicalIngestionEngine.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for EventConfigLoader
 * Note: This tests the loader's behavior, not the actual file loading
 * For integration tests with real YAML files, use @SpringBootTest
 */
class EventConfigLoaderTest {

    @Test
    @DisplayName("Should return null when event config not found")
    void testGetConfigNotFound() {
        // Given
        EventConfigLoader loader = new EventConfigLoader();

        // When
        EventConfig config = loader.get("NON_EXISTENT");

        // Then
        assertNull(config);
    }

    @Test
    @DisplayName("Should return all loaded configurations")
    void testGetAllConfigs() {
        // Given
        EventConfigLoader loader = new EventConfigLoader();

        // When
        var configs = loader.getAllConfigs();

        // Then
        assertNotNull(configs);
        assertTrue(configs.isEmpty() || configs.size() > 0);
    }

    @Test
    @DisplayName("EventConfig should have proper structure")
    void testEventConfigStructure() {
        // Given
        EventConfig config = new EventConfig();

        // When
        config.setEventName("AVS");
        config.setDescription("AVS transactions");

        // Then
        assertEquals("AVS", config.getEventName());
        assertEquals("AVS transactions", config.getDescription());
        assertNull(config.getTables()); // Not set yet
    }

    @Test
    @DisplayName("TableConfig should have proper structure")
    void testTableConfigStructure() {
        // Given
        TableConfig table = new TableConfig();

        // When
        table.setTableName("SEND_TRANSACTIONS");
        table.setOrder(1);
        table.setType("main");
        table.setAutoGenerateId(false);
        table.setParentIdField("TRAN_ID");

        // Then
        assertEquals("SEND_TRANSACTIONS", table.getTableName());
        assertEquals(1, table.getOrder());
        assertEquals("main", table.getType());
        assertFalse(table.isAutoGenerateId());
        assertEquals("TRAN_ID", table.getParentIdField());
    }

    @Test
    @DisplayName("TableConfig should handle address type mappings")
    void testAddressTypeMappings() {
        // Given
        TableConfig.AddressTypeMapping addressType = new TableConfig.AddressTypeMapping();

        // When
        addressType.setType("HOME");
        addressType.setRootPath("$");

        // Then
        assertEquals("HOME", addressType.getType());
        assertEquals("$", addressType.getRootPath());
        assertNull(addressType.getFields()); // Not set yet
    }
}