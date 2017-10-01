package com.github.ackintosh.kafka.connect;

import com.github.ackintosh.kafka.connect.model.Response;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColormeShopSourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(ColormeShopSourceTask.class);

    private ColormeShopSourceConnectorConfig config;
    private ColormeShopAPIHttpClient colormeShopAPIHttpClient;
    private int lastSaleId;
    private Instant lastMakeDate;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        config = new ColormeShopSourceConnectorConfig(map);
        colormeShopAPIHttpClient = new ColormeShopAPIHttpClient(config);
        initializeLastVariables();
    }

    private void initializeLastVariables() {
        Map<String, Object> lastSourceOffset = null;
        lastSourceOffset = context.offsetStorageReader().offset(sourcePartition());
        if (lastSourceOffset == null) {
            // we haven't fetched anything yet, so we initialize to 7 days ago
            lastSaleId = 0;
            lastMakeDate = LocalDateTime.now().minusDays(7).toInstant(ZoneOffset.UTC);
            return;
        }

        Object saleId = lastSourceOffset.get(SchemaCoordinator.SALE_ID_FIELD);
        if (saleId != null && saleId instanceof String) {
            lastSaleId = Integer.valueOf((String)saleId);
        }

        Object makeDate = lastSourceOffset.get(SchemaCoordinator.SALE_MAKE_DATE_FIELD);
        if (makeDate != null && makeDate instanceof String) {
            lastMakeDate = Instant.parse((String) makeDate);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final ArrayList<SourceRecord> records = new ArrayList<>();
        Response response = colormeShopAPIHttpClient.getNextSales(lastMakeDate);

        JSONObject meta = response.getMeta();
        log.debug(String.format("Fetched %d record(s)", meta.getInt("total")));

        for (int i = 0; i < meta.getInt("total"); i++) {
            JSONObject sale = response.getSale(i);
            if (sale.getInt(SchemaCoordinator.SALE_ID_FIELD) <= lastSaleId) {
                continue;
            }

            records.add(generateSourceRecord(sale));
            lastSaleId = sale.getInt(SchemaCoordinator.SALE_ID_FIELD);
            lastMakeDate = Instant.ofEpochSecond(sale.getInt(SchemaCoordinator.SALE_MAKE_DATE_FIELD));
        }

        Thread.sleep(1000);
        return records;
    }

    @Override
    public void stop() {
    }

    private SourceRecord generateSourceRecord(JSONObject sale) {
        return new SourceRecord(
                sourcePartition(),
                sourceOffset(sale),
                config.getTopic(),
                null, // partition will be inferred by the framework
                SchemaCoordinator.SALE_SCHEMA,
                SchemaCoordinator.buildSaleValue(sale)
        );
    }

    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put("source_partition", "colormeshop");
        return map;
    }

    private Map<String, String> sourceOffset(JSONObject sale) {
        Map<String, String> map = new HashMap<>();
        map.put(SchemaCoordinator.SALE_ID_FIELD, Integer.toString(sale.getInt(SchemaCoordinator.SALE_ID_FIELD)));
        map.put(
                SchemaCoordinator.SALE_MAKE_DATE_FIELD,
                Instant.ofEpochSecond(sale.getInt(SchemaCoordinator.SALE_MAKE_DATE_FIELD)).toString()
        );
        return map;
    }
}