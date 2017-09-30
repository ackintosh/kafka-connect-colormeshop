package com.github.ackintosh.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONArray;
import org.json.JSONObject;

class SchemaCoordinator {
    // Sale fields
    private static String SALE_ID_FIELD = "id";
    private static String SALE_ACCOUNT_ID_FIELD = "account_id";
    private static String SALE_MAKE_DATE_FIELD = "make_date";
    private static String SALE_UPDATE_DATE_FIELD = "update_date";
    private static String SALE_MOBILE_FIELD = "mobile";
    private static String SALE_PAID_FIELD = "paid";
    private static String SALE_CUSTOMER_FIELD = "customer";
    private static String SALE_DETAILS_FIELD = "details";
    private static String SALE_SALE_DELIVERIES_FIELD = "sale_deliveries";

    // Customer fields
    private static String CUSTOMER_ID_FIELD = "id";
    private static String CUSTOMER_NAME_FIELD = "name";
    private static String CUSTOMER_FURIGANA_FIELD = "furigana";

    // Details fields
    private static String DETAILES_DATA_FIELD = "data";

    // Sale deliveries fields
    private static String SALE_DELIVERIES_DATA_FIELD = "data";

    private static Schema CUSTOMER_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.customerValue")
            .version(1)
            .field(CUSTOMER_ID_FIELD, Schema.INT32_SCHEMA)
            .field(CUSTOMER_NAME_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_FURIGANA_FIELD, Schema.STRING_SCHEMA)
            .build();

    private static Schema DETAILS_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.DetailValue")
            .version(1)
            .field(DETAILES_DATA_FIELD, Schema.STRING_SCHEMA)
            .build();

    private static Schema SALE_DELIVERIES_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.SaleDeliveriesValue")
            .version(1)
            .field(SALE_DELIVERIES_DATA_FIELD, Schema.STRING_SCHEMA)
            .build();

    static Schema SALE_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.saleValue")
            .version(1)
            .field(SALE_ID_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_ACCOUNT_ID_FIELD, Schema.STRING_SCHEMA)
            .field(SALE_MAKE_DATE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_UPDATE_DATE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_MOBILE_FIELD, Schema.BOOLEAN_SCHEMA)
            .field(SALE_PAID_FIELD, Schema.BOOLEAN_SCHEMA)
            .field(SALE_CUSTOMER_FIELD, SchemaCoordinator.CUSTOMER_SCHEMA)
            .field(SALE_DETAILS_FIELD, SchemaCoordinator.DETAILS_SCHEMA)
            .field(SALE_SALE_DELIVERIES_FIELD, SchemaCoordinator.SALE_DELIVERIES_SCHEMA)
            .build();

    static Struct buildSaleValue(JSONObject sale) {
        return new Struct(SchemaCoordinator.SALE_SCHEMA)
                .put(SALE_ID_FIELD, sale.get(SALE_ID_FIELD))
                .put(SALE_ACCOUNT_ID_FIELD, sale.get(SALE_ACCOUNT_ID_FIELD))
                .put(SALE_MAKE_DATE_FIELD, sale.get(SALE_MAKE_DATE_FIELD))
                .put(SALE_UPDATE_DATE_FIELD, sale.get(SALE_UPDATE_DATE_FIELD))
                .put(SALE_MOBILE_FIELD, sale.get(SALE_MOBILE_FIELD))
                .put(SALE_PAID_FIELD, sale.get(SALE_PAID_FIELD))
                .put(SALE_CUSTOMER_FIELD, buildCustomerValue(sale.getJSONObject(SALE_CUSTOMER_FIELD)))
                .put(SALE_DETAILS_FIELD, buildDetailsValue(sale.getJSONArray(SALE_DETAILS_FIELD)))
                .put(SALE_SALE_DELIVERIES_FIELD, buildSaleDeliveriesValue(sale.getJSONArray(SALE_SALE_DELIVERIES_FIELD)));
    }

    private static Struct buildCustomerValue(JSONObject customer) {
        return new Struct(SchemaCoordinator.CUSTOMER_SCHEMA)
                .put(CUSTOMER_ID_FIELD, customer.get(CUSTOMER_ID_FIELD))
                .put(CUSTOMER_NAME_FIELD, customer.get(CUSTOMER_NAME_FIELD))
                .put(CUSTOMER_FURIGANA_FIELD, customer.get(CUSTOMER_FURIGANA_FIELD));
    }

    private static Struct buildDetailsValue(JSONArray details) {
        return new Struct(SchemaCoordinator.DETAILS_SCHEMA)
                .put(DETAILES_DATA_FIELD, details.toString());
    }

    private static Struct buildSaleDeliveriesValue(JSONArray saleDeliveries) {
        return new Struct(SchemaCoordinator.SALE_DELIVERIES_SCHEMA)
                .put(SALE_DELIVERIES_DATA_FIELD, saleDeliveries.toString());
    }
}
