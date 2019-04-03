package com.gaopeng.kafka.connect.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EnhancedTimestampRouter<R extends ConnectRecord<R>> implements Transformation<R>, AutoCloseable {

    private static final Pattern TOPIC = Pattern.compile("${topic}", Pattern.LITERAL);

    private static final Pattern TIMESTAMP = Pattern.compile("${timestamp}", Pattern.LITERAL);

    public static final String OVERVIEW_DOC =
            "Update the record's topic field as a function of the original topic value and the record timestamp."
                    + "<p/>"
                    + "This is mainly useful for sink connectors, since the topic field is often used to determine the equivalent entity name in the destination system"
                    + "(e.g. database table or search index name).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FORMAT, ConfigDef.Type.STRING, "${topic}-${timestamp}", ConfigDef.Importance.HIGH,
                    "Format string which can contain <code>${topic}</code> and <code>${timestamp}</code> as placeholders for the topic and timestamp, respectively.")
            .define(ConfigName.TIMESTAMP_FORMAT, ConfigDef.Type.STRING, "yyyyMMdd", ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.")
            .define(ConfigName.TIMESTAMP_FIELD_MAP, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "Topic to timestamp field mapping, use comma to separate each pair like topic:field.")
            .define(ConfigName.DEFAULT_TIMESTAMP_FIELD, ConfigDef.Type.STRING, "creation_time", ConfigDef.Importance.HIGH,
                    "The default timestamp field, the field value should be a long Epoch milliseconds")
            .define(ConfigName.IGNORE_TOPICS, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "The topics which do not need be transformed, use comma to separate.");

    private interface ConfigName {
        String TOPIC_FORMAT = "topic.format";
        String TIMESTAMP_FORMAT = "timestamp.format";
        String TIMESTAMP_FIELD_MAP = "timestamp.field.map";
        String DEFAULT_TIMESTAMP_FIELD = "timestamp.default.field";
        String IGNORE_TOPICS = "topics.ignore";
    }

    private String topicFormat;
    private ThreadLocal<SimpleDateFormat> timestampFormat;
    private Map<String, String> timestampFieldMap = Collections.emptyMap();
    private String defaultTimestampField;
    private Set<String> ignoreTopics = Collections.emptySet();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        topicFormat = config.getString(ConfigName.TOPIC_FORMAT);

        final String timestampFormatStr = config.getString(ConfigName.TIMESTAMP_FORMAT);
        timestampFormat = ThreadLocal.withInitial(() -> {
            final SimpleDateFormat fmt = new SimpleDateFormat(timestampFormatStr);
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
            return fmt;
        });
        final String configuredIgnoreTopics = config.getString(ConfigName.IGNORE_TOPICS);
        if (StringUtils.isNotBlank(configuredIgnoreTopics)) {
            ignoreTopics = Arrays.stream(configuredIgnoreTopics.split("\\s*,\\s*")).collect(Collectors.toSet());
        }
        defaultTimestampField = config.getString(ConfigName.DEFAULT_TIMESTAMP_FIELD);
        final String configuredTimestampFiledMap = config.getString(ConfigName.TIMESTAMP_FIELD_MAP);
        if (StringUtils.isNotBlank(configuredTimestampFiledMap)) {
            timestampFieldMap = Arrays.stream(configuredTimestampFiledMap.split("\\s*,\\s*"))
                    .map(pair -> pair.split(":"))
                    .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));
        }
    }

    @Override
    public R apply(R record) {
        final String topic = record.topic();
        if (ignoreTopics.contains(topic)) {
            return record;
        }
        final String field = timestampFieldMap.containsKey(topic) ? timestampFieldMap.get(topic) : defaultTimestampField;
        Long timestamp = record.timestamp();
        if (record.value() instanceof Map) {
            final Map<String, Object> values = Requirements.requireMapOrNull(record.value(), "format timestamp");
            if (values != null && values.containsKey(field)) {
                timestamp = (Long) values.get(field);
            }
        } else if (record.value() instanceof Struct) {
            Struct struct = Requirements.requireStructOrNull(record.value(), "format timestamp");
            if (struct != null && struct.getInt64(field) != null) {
                timestamp = struct.getInt64(field);
            }
        }
        if (timestamp == null) {
            throw new DataException("Timestamp field \"" + field + "\" missing on record value: " + record);
        }
        final String formattedTimestamp = timestampFormat.get().format(new Date(timestamp));

        final String replace1 = TOPIC.matcher(topicFormat).replaceAll(Matcher.quoteReplacement(topic));
        final String updatedTopic = TIMESTAMP.matcher(replace1).replaceAll(Matcher.quoteReplacement(formattedTimestamp));
        return record.newRecord(
                updatedTopic, record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), record.value(),
                record.timestamp()
        );
    }

    @Override
    public void close() {
        timestampFormat = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
