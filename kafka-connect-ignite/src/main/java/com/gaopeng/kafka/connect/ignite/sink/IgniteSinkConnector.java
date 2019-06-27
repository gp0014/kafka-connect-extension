package com.gaopeng.kafka.connect.ignite.sink;

import com.gaopeng.kafka.connect.common.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IgniteSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(IgniteSinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) {
        try {
            configProperties = props;
            // validation
            new IgniteSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start IgniteSinkConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IgniteSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProperties);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.warn("Stopping IgniteSinkConnector.");
    }

    @Override
    public ConfigDef config() {
        return IgniteSinkConnectorConfig.CONFIG;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
