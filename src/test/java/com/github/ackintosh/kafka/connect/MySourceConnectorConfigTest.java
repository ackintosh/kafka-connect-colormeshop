package com.github.ackintosh.kafka.connect;

import org.junit.Test;

public class MySourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(MySourceConnectorConfig.conf().toRst());
  }
}