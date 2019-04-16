package com.gaopeng.kafka.connect.transform;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FieldRouter<R extends ConnectRecord<R>> implements Transformation<R>, AutoCloseable {

    private static final Pattern TOPIC = Pattern.compile("${topic}", Pattern.LITERAL);

    private static final Pattern VALUE = Pattern.compile("${value}", Pattern.LITERAL);

    public static final String OVERVIEW_DOC =
            "Update the record's topic field as a function of the original topic value and the specified field value of a map or struct"
                    + "<p/>"
                    + "This is mainly useful for sink connectors, since the topic field is often used to determine the equivalent entity name in the destination system"
                    + "(e.g. database table or search index name).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FORMAT, ConfigDef.Type.STRING, "${topic}-${value}", ConfigDef.Importance.HIGH,
                    "Format string which can contain <code>${topic}</code> and <code>${value}</code> as placeholders for the topic and value, respectively.")
            .define(ConfigName.EXPRESSION, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "The expression is used to calculate the filed value, it's useful when you want to convert the original filed value to another one. " +
                            "The original field value will be a variable named \"value\" in expression engine. " +
                            "The expression engine is Aviator, please check the doc to get help")
            .define(ConfigName.TOPIC_FIELD_MAP, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "Topic to field mapping, use comma to separate each pair like topic:field.")
            .define(ConfigName.DEFAULT_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "The default field, the field value should be a non null value")
            .define(ConfigName.IGNORE_TOPICS, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                    "The topics which do not need be transformed, use comma to separate.");

    private interface ConfigName {
        String TOPIC_FORMAT = "topic.format";
        String EXPRESSION = "value.expression";
        String TOPIC_FIELD_MAP = "topics.field.map";
        String DEFAULT_FIELD = "default.field";
        String IGNORE_TOPICS = "topics.ignore";
    }

    private String topicFormat;
    private AviatorEvaluatorInstance evaluator = AviatorEvaluator.newInstance();
    private String expression;
    private Map<String, String> topicFieldMap = Collections.emptyMap();
    private String defaultField;
    private Set<String> ignoreTopics = Collections.emptySet();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        topicFormat = config.getString(ConfigName.TOPIC_FORMAT);

        expression = config.getString(ConfigName.EXPRESSION);
        final String configuredIgnoreTopics = config.getString(ConfigName.IGNORE_TOPICS);
        if (StringUtils.isNotBlank(configuredIgnoreTopics)) {
            ignoreTopics = Arrays.stream(configuredIgnoreTopics.split("\\s*,\\s*")).collect(Collectors.toSet());
        }
        defaultField = config.getString(ConfigName.DEFAULT_FIELD);
        final String configuredTopicFiledMap = config.getString(ConfigName.TOPIC_FIELD_MAP);
        if (StringUtils.isNotBlank(configuredTopicFiledMap)) {
            topicFieldMap = Arrays.stream(configuredTopicFiledMap.split("\\s*,\\s*"))
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
        final String field = topicFieldMap.containsKey(topic) ? topicFieldMap.get(topic) : defaultField;
        if (StringUtils.isEmpty(field)) {
            throw new DataException(String.format("You must configure at least \"%s\" or \"%s\"", ConfigName.TOPIC_FIELD_MAP, ConfigName.DEFAULT_FIELD));
        }
        Object fieldValue = null;
        if (record.value() instanceof Map) {
            final Map<String, Object> values = Requirements.requireMapOrNull(record.value(), "transform topic");
            if (values != null && values.containsKey(field)) {
                fieldValue = values.get(field);
            }
        } else if (record.value() instanceof Struct) {
            Struct struct = Requirements.requireStructOrNull(record.value(), "transform topic");
            if (struct != null && struct.get(field) != null) {
                fieldValue = struct.get(field);
            }
        } else {
            throw new DataException("The record is neither a Map nor a Struct");
        }

        if (fieldValue == null) {
            throw new DataException("The given field value is null or the filed does not exist");
        }

        if (StringUtils.isNotEmpty(expression)) {
            Map<String, Object> env = new HashMap<>();
            env.put("value", fieldValue);
            Object result = evaluator.execute(expression, env);
            fieldValue = result == null ? StringUtils.EMPTY : result.toString();
        }

        final String replace1 = TOPIC.matcher(topicFormat).replaceAll(Matcher.quoteReplacement(topic));
        final String updatedTopic = VALUE.matcher(replace1).replaceAll(Matcher.quoteReplacement(fieldValue.toString()));
        return record.newRecord(
                updatedTopic, record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), record.value(),
                record.timestamp()
        );
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

}
