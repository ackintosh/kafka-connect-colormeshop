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

    public ColormeShopAPIHttpClient() {

    }

    protected JSONArray getNextSales() throws InterruptedException {
        GetRequest unirest = Unirest.get("https://api.shop-pro.jp/v1/sales.json");
        log.debug(String.format("GET %s", unirest.getUrl()));

        try {
            HttpResponse<JsonNode> response = unirest.asJson();
        } catch (UnirestException e) {
            e.printStackTrace();
            return new JSONArray();
        }
        return null;
    }
}
