package com.github.ackintosh.kafka.connect;

import com.github.ackintosh.kafka.connect.model.Response;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class ColormeShopAPIHttpClient {
    private static final Logger log = LoggerFactory.getLogger(ColormeShopAPIHttpClient.class);
    private ColormeShopSourceConnectorConfig config;

    public ColormeShopAPIHttpClient(ColormeShopSourceConnectorConfig config) {
        this.config = config;
    }

    protected Response getNextSales(Instant lastMakeDate) throws InterruptedException {
        String url = String.format(
                "https://api.shop-pro.jp/v1/sales.json?after=%s",
                lastMakeDate.toString()
        );
        GetRequest request = Unirest.get(url)
                .header("Authorization", "Bearer " + config.getAccessToken());
        log.debug(String.format("GET %s", request.getUrl()));

        try {
            HttpResponse<JsonNode> response = request.asJson();
            log.debug(String.format("%d: %s", response.getStatus(), response.getStatusText()));
            return new Response(response.getBody().getObject());
        } catch (UnirestException e) {
            e.printStackTrace();
            return new Response(new JSONObject());
        }
    }
}
