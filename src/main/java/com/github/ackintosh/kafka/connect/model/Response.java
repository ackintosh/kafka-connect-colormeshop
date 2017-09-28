package com.github.ackintosh.kafka.connect.model;


import org.json.JSONArray;
import org.json.JSONObject;

public class Response {
    private JSONObject data;
    private JSONArray sales;

    public Response(JSONObject data) {
        this.data = data;
        sales = data.getJSONArray("sales");
    }

    public JSONObject getSale(int index) {
        if (sales.isNull(index)) {
            // TODO: error handling
            return new JSONObject();
        }

        return sales.getJSONObject(index);
    }

    public JSONObject getMeta() {
        return new JSONObject(data.get("meta").toString());
    }
}
