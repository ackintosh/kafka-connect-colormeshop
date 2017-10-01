package com.github.ackintosh.kafka.connect;

import com.github.ackintosh.kafka.connect.model.Response;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class ColormeShopAPIHttpClientTest {
    @Test
    public void getNextSales() {
        String accessToken = System.getProperty("access_token");
        if (accessToken == null) {
            Assert.fail();
        }

        Map<String, String> map = new HashMap();
        map.put("topic", "test_topic");
        map.put("access_token", accessToken);
        ColormeShopSourceConnectorConfig config = new ColormeShopSourceConnectorConfig(map);
        ColormeShopAPIHttpClient client = new ColormeShopAPIHttpClient(config);

        try {
            Response response = client.getNextSales(
                    LocalDateTime.now().minusDays(7).toInstant(ZoneOffset.UTC)
            );
            Assert.assertTrue(response.getMeta().getInt("total") > 0);
        } catch (InterruptedException e) {
            Assert.fail();
        }
    }
}
