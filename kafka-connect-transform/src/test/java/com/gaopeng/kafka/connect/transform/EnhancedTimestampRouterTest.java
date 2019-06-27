package com.gaopeng.kafka.connect.transform;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EnhancedTimestampRouterTest {

    private final EnhancedTimestampRouter<SourceRecord> xform = new EnhancedTimestampRouter<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test(expected= DataException.class)
    public void testDefaultConfiguration() {
        xform.configure(Collections.emptyMap()); // defaults
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, null,
                1483425001864L
        );
        xform.apply(record);
//        assertEquals("test-20170103", xform.apply(record).topic());
    }

    @Test
    public void testIgnoreTopics() {
        Map<String, Object> config = new HashMap<>();
        config.put("topics.ignore", "test");
        xform.configure(config);
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, null,
                1483425001864L
        );
        assertEquals("test", xform.apply(record).topic());
    }

    @Test
    public void testTimestampFieldMap() {
        Map<String, Object> config = new HashMap<>();
        config.put("timestamp.field.map", "test:created_at");
        xform.configure(config);
        Map<String, Object> value = new HashMap<>();
        value.put("created_at", 1554190424000L); // 2019-04-02
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        assertEquals("test-20190402", xform.apply(record).topic());
    }

    @Test
    public void testDefaultTimestampFiled() {
        Map<String, Object> config = new HashMap<>();
        config.put("timestamp.field.map", "test1:created_at");
        xform.configure(config);
        Map<String, Object> value = new HashMap<>();
        value.put("created_at", 1554190424000L);  // 2019-04-02
        value.put("creation_time", 1554104024000L);  // 2019-04-01
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        assertEquals("test-20190401", xform.apply(record).topic());
    }
}
