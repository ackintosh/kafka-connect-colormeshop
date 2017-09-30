package com.github.ackintosh.kafka.connect;

import com.github.ackintosh.kafka.connect.model.Response;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;
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
      Response response = colormeShopAPIHttpClient.getNextSales();

      JSONObject meta = response.getMeta();
      log.debug(String.format("Fetched %d record(s)", meta.getInt("total")));

      for (int i = 0; i < meta.getInt("total"); i++) {
          records.add(generateSourceRecord(response.getSale(i)));
      }

      return records;
  }

  @Override
  public void stop() {
      //TODO: Do whatever is required to stop your task.
  }

  private SourceRecord generateSourceRecord(JSONObject sale) {
      return new SourceRecord(
              sourcePartition(),
              sourceOffset(),
              "mysourcetopic",
              null, // partition will be inferred by the framework
              SchemaCoordinator.SALE_SCHEMA,
              buildRecordValue(sale)
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

  private Struct buildRecordValue(JSONObject sale) {
      return new Struct(SchemaCoordinator.SALE_SCHEMA)
              .put("testvalue", sale.toString());
  }
}