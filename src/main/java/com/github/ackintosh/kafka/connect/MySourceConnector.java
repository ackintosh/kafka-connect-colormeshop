package com.github.ackintosh.kafka.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySourceConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(MySourceConnector.class);
  private MySourceConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new MySourceConnectorConfig(map);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MySourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    ArrayList<Map<String, String>> configs = new ArrayList<>(1);
    configs.add(config.originalsStrings());

    return configs;
  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return MySourceConnectorConfig.conf();
  }
}
