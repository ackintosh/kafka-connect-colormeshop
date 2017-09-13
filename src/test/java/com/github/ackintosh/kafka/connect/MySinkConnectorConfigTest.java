package com.github.ackintosh.kafka.connect;

import org.junit.Test;

public class MySinkConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(MySinkConnectorConfig.conf().toRst());
  }
}
