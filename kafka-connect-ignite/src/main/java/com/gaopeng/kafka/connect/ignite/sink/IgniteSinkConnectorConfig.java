package com.gaopeng.kafka.connect.ignite.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class IgniteSinkConnectorConfig extends AbstractConfig {

    public static final ConfigDef CONFIG = baseConfigDef();

    public IgniteSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }

    private static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectorConfigs(configDef);
        addConversionConfigs(configDef);
        return configDef;
    }

    private static void addConversionConfigs(ConfigDef configDef) {

    }

    private static void addConnectorConfigs(ConfigDef configDef) {
    }
}
