package com.gaopeng.kafka.connect.transform;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FieldRouterTest {

    private final FieldRouter<SourceRecord> xform = new FieldRouter<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void testDefaultConfiguration() {
        xform.configure(Collections.emptyMap()); // defaults
        Map<String, Object> value = new HashMap<>();
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        try {
            xform.apply(record);
        } catch (Exception e) {
            assertEquals("You must configure at least \"topics.field.map\" or \"default.field\"", e.getLocalizedMessage());
        }
    }

    @Test
    public void testNullField() {
        Map<String, Object> config = new HashMap<>();
        config.put("default.field", "task_id");
        xform.configure(config);
        Map<String, Object> value = new HashMap<>();
        value.put("user_id", 987654321);
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        try {
            xform.apply(record);
        } catch (Exception e) {
            assertEquals("The given field value is null or the filed does not exist", e.getLocalizedMessage());
        }
    }

    @Test
    public void testNonMapOrStructValue() {
        Map<String, Object> config = new HashMap<>();
        config.put("default.field", "task_id");
        xform.configure(config);
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, "hello world",
                1483425001864L
        );
        try {
            xform.apply(record);
        } catch (Exception e) {
            assertEquals("The record is neither a Map nor a Struct", e.getLocalizedMessage());
        }
    }

    @Test
    public void testStructValue() {
        Map<String, Object> config = new HashMap<>();
        config.put("default.field", "task_id");
        config.put("value.expression", "value % 5");
        xform.configure(config);
        Schema schema = SchemaBuilder.struct()
                .field("user_id",SchemaBuilder.int32())
                .field("task_id",SchemaBuilder.int32());
        Struct value = new Struct(schema);
        value.put("user_id", 987654321);
        value.put("task_id", 123456789);
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        assertEquals("test-4", xform.apply(record).topic());
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
    public void testTopicFieldMap() {
        Map<String, Object> config = new HashMap<>();
        config.put("topics.field.map", "test:user_id");
        xform.configure(config);
        Map<String, Object> value = new HashMap<>();
        value.put("user_id", 987654321);
        value.put("task_id", 123456789);
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        assertEquals("test-987654321", xform.apply(record).topic());
    }

    @Test
    public void testDefaultTimestampFiled() {
        Map<String, Object> config = new HashMap<>();
        config.put("default.field", "task_id");
        xform.configure(config);
        Map<String, Object> value = new HashMap<>();
        value.put("user_id", 987654321);
        value.put("task_id", 123456789);
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        assertEquals("test-123456789", xform.apply(record).topic());


        config.put("topics.field.map", "test:user_id");
        xform.configure(config);
        final SourceRecord record2 = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        assertEquals("test-987654321", xform.apply(record).topic());
    }

    @Test
    public void testExpression() {
        Map<String, Object> config = new HashMap<>();
        config.put("default.field", "task_id");
        config.put("value.expression", "string.substring(value,string.length(value)-1)");
        xform.configure(config);
        Map<String, Object> value = new HashMap<>();
        value.put("user_id", 987654321);
        value.put("task_id", "123456789");
        final SourceRecord record = new SourceRecord(
                null, null,
                "test", 0,
                null, null,
                null, value,
                1483425001864L
        );
        assertEquals("test-9", xform.apply(record).topic());
    }
}
