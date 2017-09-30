package com.github.ackintosh.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

class SchemaCoordinator {
    static Schema SALE_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.saleValue")
            .version(1)
            .field("id", Schema.INT64_SCHEMA)
            .field("account_id", Schema.STRING_SCHEMA)
            .field("make_date", Schema.INT64_SCHEMA)
            .field("update_date", Schema.INT64_SCHEMA)
            .field("mobile", Schema.BOOLEAN_SCHEMA)
            .field("paid", Schema.BOOLEAN_SCHEMA)
            .field("customer", SchemaCoordinator.CUSTOMER_SCHEMA)
            .field("details", SchemaCoordinator.DETAILS_SCHEMA)
            .field("sale_deliveries", SchemaCoordinator.SALE_DELIVERIES_SCHEMA)
            .build();

    private static Schema CUSTOMER_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.customerValue")
            .version(1)
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("furigana", Schema.STRING_SCHEMA)
            .build();

    private static Schema DETAILS_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.DetailValue")
            .field("data", Schema.STRING_SCHEMA)
            .build();

    private static Schema SALE_DELIVERIES_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.SaleDeliveriesValue")
            .field("data", Schema.STRING_SCHEMA)
            .build();
}
