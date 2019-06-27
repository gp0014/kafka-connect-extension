package com.gaopeng.kafka.connect.ignite.sink;

import com.gaopeng.kafka.connect.common.Version;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class IgniteSinkTask extends SinkTask {
    @Override
    public String version() {
        return Version.getVersion() ;
    }

    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public void put(Collection<SinkRecord> collection) {

    }

    @Override
    public void stop() {

    }
}
