package com.github.ackintosh.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColormeShopSourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(ColormeShopSourceTask.class);

  private ColormeShopSourceConnectorConfig config;
  private ColormeShopAPIHttpClient colormeShopAPIHttpClient;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
      System.out.println("------- start --------");
      config = new ColormeShopSourceConnectorConfig(map);
      colormeShopAPIHttpClient = new ColormeShopAPIHttpClient(config);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
      System.out.println("------- poll --------");
      final ArrayList<SourceRecord> records = new ArrayList<>();
      JSONArray sales = colormeShopAPIHttpClient.getNextSales();
      log.debug(sales.toString());
      records.add(generateSourceRecord(sales));

      return records;
  }

  @Override
  public void stop() {
      //TODO: Do whatever is required to stop your task.
  }

  private SourceRecord generateSourceRecord(JSONArray sales) {
      return new SourceRecord(
              sourcePartition(),
              sourceOffset(),
              "mysourcetopic",
              null, // partition will be inferred by the framework
              buildValueSchema(),
              buildRecordValue(sales)
      );
  }

  private Map<String, String> sourcePartition() {
      Map<String, String> map = new HashMap<>();
      map.put("account_id", "test");
      return map;
  }

  private Map<String, String> sourceOffset() {
      Map<String, String> map = new HashMap<>();
      map.put("created_at", "2017-09-23");
      return map;
  }

  private Schema buildValueSchema() {
      return SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.value")
              .version(1)
              .field("testvalue", Schema.STRING_SCHEMA)
              .build();
  }

  private Struct buildRecordValue(JSONArray sales) {
      return new Struct(buildValueSchema())
              .put("testvalue", sales.toString());
  }
}