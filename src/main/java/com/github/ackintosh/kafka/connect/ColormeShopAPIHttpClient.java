package com.github.ackintosh.kafka.connect;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColormeShopAPIHttpClient {
    private static final Logger log = LoggerFactory.getLogger(ColormeShopAPIHttpClient.class);
    private ColormeShopSourceConnectorConfig config;

    public ColormeShopAPIHttpClient(ColormeShopSourceConnectorConfig config) {
        this.config = config;
    }

    protected JSONArray getNextSales() throws InterruptedException {
        GetRequest request = Unirest.get("https://api.shop-pro.jp/v1/sales.json")
                .header("Authorization", "Bearer " + config.getAccessToken());
        log.debug(String.format("GET %s", request.getUrl()));

        try {
            HttpResponse<JsonNode> response = request.asJson();
            log.debug(String.format("%d: %s", response.getStatus(), response.getStatusText()));
            return response.getBody().getArray();
        } catch (UnirestException e) {
            e.printStackTrace();
            return new JSONArray();
        }
    }
}
