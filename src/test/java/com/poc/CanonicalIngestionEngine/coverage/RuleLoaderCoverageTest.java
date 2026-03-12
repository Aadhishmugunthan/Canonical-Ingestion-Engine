package com.poc.CanonicalIngestionEngine.coverage;

import com.poc.CanonicalIngestionEngine.rules.RuleLoader;
import org.jeasy.rules.api.Rules;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class RuleLoaderCoverageTest {

    // 🔥 covers init() + missing folder branch
    @Test
    void shouldExecuteInitWithoutCrashing() {

        RuleLoader loader = new RuleLoader();
        loader.init();

        Set<String> types = loader.getLoadedEventTypes();
        assertNotNull(types);
    }

    // 🔥 covers getRules(null)
    @Test
    void shouldReturnEmptyRulesWhenNullEvent() {

        RuleLoader loader = new RuleLoader();
        Rules rules = loader.getRules(null);

        assertEquals(0, rules.size());
    }

    // 🔥 covers event not found branch
    @Test
    void shouldReturnEmptyRulesWhenEventNotFound() {

        RuleLoader loader = new RuleLoader();
        Rules rules = loader.getRules("UNKNOWN");

        assertEquals(0, rules.size());
    }

    // 🔥 force cache to cover success branch
    @Test
    void shouldReturnRulesWhenPresent() throws Exception {

        RuleLoader loader = new RuleLoader();

        Field field = RuleLoader.class.getDeclaredField("rulesCache");
        field.setAccessible(true);

        Map<String, Rules> cache = (Map<String, Rules>) field.get(loader);

        Rules dummy = new Rules();
        cache.put("AVS", dummy);

        Rules result = loader.getRules("avs");

        assertEquals(dummy, result);
    }

    // 🔥 cover private method extractEventType
    @Test
    void shouldCoverExtractEventType() throws Exception {

        RuleLoader loader = new RuleLoader();

        Method m = RuleLoader.class
                .getDeclaredMethod("extractEventType", String.class);

        m.setAccessible(true);

        String res = (String) m.invoke(loader, "avs-rules.yml");

        assertEquals("AVS", res);
    }
}