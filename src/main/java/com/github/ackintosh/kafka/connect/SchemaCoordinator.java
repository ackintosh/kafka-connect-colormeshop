package com.github.ackintosh.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.util.Date;

class SchemaCoordinator {
    // Sale fields
    private static String SALE_ID_FIELD = "id";
    private static String SALE_ACCOUNT_ID_FIELD = "account_id";
    private static String SALE_MAKE_DATE_FIELD = "make_date";
    private static String SALE_UPDATE_DATE_FIELD = "update_date";
    private static String SALE_MOBILE_FIELD = "mobile";
    private static String SALE_PAID_FIELD = "paid";
    private static String SALE_DELIVERED_FIELD = "delivered";
    private static String SALE_CANCELED_FIELD = "canceled";
    private static String SALE_PRODUCT_TOTAL_PRICE_FIELD = "product_total_price";
    private static String SALE_DELIVERY_TOTAL_CHARGE_FIELD = "delivery_total_charge";
    private static String SALE_TAX_FIELD = "tax";
    private static String SALE_FEE_FIELD = "fee";
    private static String SALE_NOSHI_TOTAL_CHARGE_FIELD = "noshi_total_charge";
    private static String SALE_CARD_TOTAL_CHARGE_FIELD = "card_total_charge";
    private static String SALE_WRAPPING_TOTAL_CHAGE_FIELD = "wrapping_total_charge";
    private static String SALE_POINT_DISCOUNT_FIELD = "point_discount";
    private static String SALE_GMO_POINT_DISCOUNT_FIELD = "gmo_point_discount";
    private static String SALE_OTHER_DISCOUNT_FIELD = "other_discount";
    private static String SALE_OTHER_DISCOUNT_NAME_FIELD = "other_discount_name";
    private static String SALE_TOTAL_PRICE_FIELD = "total_price";
    private static String SALE_POINT_STATE_FIELD = "point_state";
    private static String SALE_GRANTED_POINTS_FIELD = "granted_points";
    private static String SALE_USE_POINTS_FIELD = "use_points";
    private static String SALE_GMO_POINT_STATE_FIELD = "gmo_point_state";
    private static String SALE_GRANTED_GMO_POINTS_FIELD = "granted_gmo_points";
    private static String SALE_USE_GMO_POINTS_FIELD = "use_gmo_points";
    private static String SALE_ACCEPTED_MAIL_STATE_FIELD = "accepted_mail_state";
    private static String SALE_ACCEPTED_MAIL_SENT_DATE_FIELD = "accepted_mail_sent_date";
    private static String SALE_PAID_MAIL_STATE_FIELD = "paid_mail_state";
    private static String SALE_PAID_MAIL_SENT_DATE_FIELD = "paid_mail_sent_date";
    private static String SALE_DELIVERED_MAIL_STATE_FIELD = "delivered_mail_state";
    private static String SALE_DELIVERED_MAIL_SENT_DATE_FIELD = "delivered_mail_sent_date";
    private static String SALE_PAYMENT_ID_FIELD = "payment_id";
    private static String SALE_CUSTOMER_FIELD = "customer";
    private static String SALE_DETAILS_FIELD = "details";
    private static String SALE_SALE_DELIVERIES_FIELD = "sale_deliveries";

    // Customer fields
    private static String CUSTOMER_ID_FIELD = "id";
    private static String CUSTOMER_NAME_FIELD = "name";
    private static String CUSTOMER_FURIGANA_FIELD = "furigana";
    private static String CUSTOMER_HOJIN_FIELD = "hojin";
    private static String CUSTOMER_BUSHO_FIELD = "busho";
    private static String CUSTOMER_POSTAL_FIELD = "postal";
    private static String CUSTOMER_PREF_ID_FIELD = "pref_id";
    private static String CUSTOMER_PREF_NAME_FIELD = "pref_name";
    private static String CUSTOMER_ADDRESS1_FIELD = "address1";
    private static String CUSTOMER_ADDRESS2_FIELD = "address2";
    private static String CUSTOMER_MAIL_FIELD = "mail";
    private static String CUSTOMER_TEL_FIELD = "tel";
    private static String CUSTOMER_FAX_FIELD = "fax";
    private static String CUSTOMER_TEL_MOBILE_FIELD = "tel_mobile";
    private static String CUSTOMER_MEMO_FIELD = "memo";
    private static String CUSTOMER_SEX_FIELD = "sex";
    private static String CUSTOMER_MEMBER_FIELD = "member";
    private static String CUSTOMER_SALES_COUNT_FIELD = "sales_count";
    private static String CUSTOMER_POINTS_FIELD = "points";

    // Details fields
    private static String DETAILES_DATA_FIELD = "data";

    // Sale deliveries fields
    private static String SALE_DELIVERIES_DATA_FIELD = "data";

    private static Schema CUSTOMER_SCHEMA = SchemaBuilder.struct().name("com.github.ackintosh.kafka.connect.customerValue")
            .version(1)
            .field(CUSTOMER_ID_FIELD, Schema.INT32_SCHEMA)
            .field(CUSTOMER_NAME_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_FURIGANA_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_HOJIN_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_BUSHO_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_POSTAL_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_PREF_ID_FIELD, Schema.INT32_SCHEMA)
            .field(CUSTOMER_PREF_NAME_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_ADDRESS1_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_ADDRESS2_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_MAIL_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_TEL_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_FAX_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_TEL_MOBILE_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_MEMO_FIELD, Schema.STRING_SCHEMA)
            .field(CUSTOMER_SEX_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CUSTOMER_MEMBER_FIELD, Schema.BOOLEAN_SCHEMA)
            .field(CUSTOMER_SALES_COUNT_FIELD, Schema.INT32_SCHEMA)
            .field(CUSTOMER_POINTS_FIELD, Schema.INT32_SCHEMA)
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
            .field(SALE_MAKE_DATE_FIELD, Timestamp.SCHEMA)
            .field(SALE_UPDATE_DATE_FIELD, Timestamp.SCHEMA)
            .field(SALE_MOBILE_FIELD, Schema.BOOLEAN_SCHEMA)
            .field(SALE_PAID_FIELD, Schema.BOOLEAN_SCHEMA)
            .field(SALE_DELIVERED_FIELD, Schema.BOOLEAN_SCHEMA)
            .field(SALE_CANCELED_FIELD, Schema.BOOLEAN_SCHEMA)
            .field(SALE_PRODUCT_TOTAL_PRICE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_DELIVERY_TOTAL_CHARGE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_TAX_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_FEE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_NOSHI_TOTAL_CHARGE_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
            .field(SALE_CARD_TOTAL_CHARGE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_WRAPPING_TOTAL_CHAGE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_POINT_DISCOUNT_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_GMO_POINT_DISCOUNT_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_OTHER_DISCOUNT_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_OTHER_DISCOUNT_NAME_FIELD, Schema.STRING_SCHEMA)
            .field(SALE_TOTAL_PRICE_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_POINT_STATE_FIELD, Schema.STRING_SCHEMA)
            .field(SALE_GRANTED_POINTS_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_USE_POINTS_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_GMO_POINT_STATE_FIELD, Schema.STRING_SCHEMA)
            .field(SALE_GRANTED_GMO_POINTS_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_USE_GMO_POINTS_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_ACCEPTED_MAIL_STATE_FIELD, Schema.STRING_SCHEMA)
            .field(SALE_ACCEPTED_MAIL_SENT_DATE_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
            .field(SALE_PAID_MAIL_STATE_FIELD, Schema.STRING_SCHEMA)
            .field(SALE_PAID_MAIL_SENT_DATE_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
            .field(SALE_DELIVERED_MAIL_STATE_FIELD, Schema.STRING_SCHEMA)
            .field(SALE_DELIVERED_MAIL_SENT_DATE_FIELD, Schema.OPTIONAL_INT32_SCHEMA)
            .field(SALE_PAYMENT_ID_FIELD, Schema.INT32_SCHEMA)
            .field(SALE_CUSTOMER_FIELD, SchemaCoordinator.CUSTOMER_SCHEMA)
            .field(SALE_DETAILS_FIELD, SchemaCoordinator.DETAILS_SCHEMA)
            .field(SALE_SALE_DELIVERIES_FIELD, SchemaCoordinator.SALE_DELIVERIES_SCHEMA)
            .build();

    static Struct buildSaleValue(JSONObject sale) {
        Struct saleValue = new Struct(SchemaCoordinator.SALE_SCHEMA)
                .put(SALE_ID_FIELD, sale.get(SALE_ID_FIELD))
                .put(SALE_ACCOUNT_ID_FIELD, sale.get(SALE_ACCOUNT_ID_FIELD))
                .put(SALE_MAKE_DATE_FIELD, Date.from(Instant.ofEpochSecond(sale.getInt(SALE_MAKE_DATE_FIELD))))
                .put(SALE_UPDATE_DATE_FIELD, Date.from(Instant.ofEpochSecond(sale.getInt(SALE_UPDATE_DATE_FIELD))))
                .put(SALE_MOBILE_FIELD, sale.get(SALE_MOBILE_FIELD))
                .put(SALE_PAID_FIELD, sale.get(SALE_PAID_FIELD))
                .put(SALE_DELIVERED_FIELD, sale.get(SALE_DELIVERED_FIELD))
                .put(SALE_CANCELED_FIELD, sale.get(SALE_CANCELED_FIELD))
                .put(SALE_PRODUCT_TOTAL_PRICE_FIELD, sale.get(SALE_PRODUCT_TOTAL_PRICE_FIELD))
                .put(SALE_DELIVERY_TOTAL_CHARGE_FIELD, sale.get(SALE_DELIVERY_TOTAL_CHARGE_FIELD))
                .put(SALE_TAX_FIELD, sale.get(SALE_TAX_FIELD))
                .put(SALE_FEE_FIELD, sale.get(SALE_FEE_FIELD))
                .put(SALE_NOSHI_TOTAL_CHARGE_FIELD, sale.get(SALE_NOSHI_TOTAL_CHARGE_FIELD))
                .put(SALE_CARD_TOTAL_CHARGE_FIELD, sale.get(SALE_CARD_TOTAL_CHARGE_FIELD))
                .put(SALE_WRAPPING_TOTAL_CHAGE_FIELD, sale.get(SALE_WRAPPING_TOTAL_CHAGE_FIELD))
                .put(SALE_POINT_DISCOUNT_FIELD, sale.get(SALE_POINT_DISCOUNT_FIELD))
                .put(SALE_GMO_POINT_DISCOUNT_FIELD, sale.get(SALE_GMO_POINT_DISCOUNT_FIELD))
                .put(SALE_OTHER_DISCOUNT_FIELD, sale.get(SALE_OTHER_DISCOUNT_FIELD))
                .put(SALE_OTHER_DISCOUNT_NAME_FIELD, sale.get(SALE_OTHER_DISCOUNT_NAME_FIELD))
                .put(SALE_POINT_STATE_FIELD, sale.get(SALE_POINT_STATE_FIELD))
                .put(SALE_TOTAL_PRICE_FIELD, sale.get(SALE_TOTAL_PRICE_FIELD))
                .put(SALE_GRANTED_POINTS_FIELD, sale.get(SALE_GRANTED_POINTS_FIELD))
                .put(SALE_USE_POINTS_FIELD, sale.get(SALE_USE_POINTS_FIELD))
                .put(SALE_GMO_POINT_STATE_FIELD, sale.get(SALE_GMO_POINT_STATE_FIELD))
                .put(SALE_GRANTED_GMO_POINTS_FIELD, sale.get(SALE_GRANTED_GMO_POINTS_FIELD))
                .put(SALE_USE_GMO_POINTS_FIELD, sale.get(SALE_USE_GMO_POINTS_FIELD))
                .put(SALE_ACCEPTED_MAIL_STATE_FIELD, sale.get(SALE_ACCEPTED_MAIL_STATE_FIELD))
                .put(SALE_PAID_MAIL_STATE_FIELD, sale.get(SALE_PAID_MAIL_STATE_FIELD))
                .put(SALE_DELIVERED_MAIL_STATE_FIELD, sale.get(SALE_DELIVERED_MAIL_STATE_FIELD))
                .put(SALE_PAYMENT_ID_FIELD, sale.get(SALE_PAYMENT_ID_FIELD))
                .put(SALE_CUSTOMER_FIELD, buildCustomerValue(sale.getJSONObject(SALE_CUSTOMER_FIELD)))
                .put(SALE_DETAILS_FIELD, buildDetailsValue(sale.getJSONArray(SALE_DETAILS_FIELD)))
                .put(SALE_SALE_DELIVERIES_FIELD, buildSaleDeliveriesValue(sale.getJSONArray(SALE_SALE_DELIVERIES_FIELD)));

        Object acceptedMailSentDate = sale.get(SALE_ACCEPTED_MAIL_SENT_DATE_FIELD);
        if (!acceptedMailSentDate.equals(null)) {
            saleValue.put(SALE_ACCEPTED_MAIL_SENT_DATE_FIELD, acceptedMailSentDate);
        }

        Object paidMailSentDate = sale.get(SALE_PAID_MAIL_SENT_DATE_FIELD);
        if (!paidMailSentDate.equals(null)) {
            saleValue.put(SALE_PAID_MAIL_SENT_DATE_FIELD, paidMailSentDate);
        }

        Object deliveredMailSentDate = sale.get(SALE_DELIVERED_MAIL_SENT_DATE_FIELD);
        if (!deliveredMailSentDate.equals(null)) {
            saleValue.put(SALE_DELIVERED_MAIL_SENT_DATE_FIELD, deliveredMailSentDate);
        }

        return saleValue;
    }

    private static Struct buildCustomerValue(JSONObject customer) {
        Struct customerValue = new Struct(SchemaCoordinator.CUSTOMER_SCHEMA)
                .put(CUSTOMER_ID_FIELD, customer.get(CUSTOMER_ID_FIELD))
                .put(CUSTOMER_NAME_FIELD, customer.get(CUSTOMER_NAME_FIELD))
                .put(CUSTOMER_FURIGANA_FIELD, customer.get(CUSTOMER_FURIGANA_FIELD))
                .put(CUSTOMER_HOJIN_FIELD, customer.get(CUSTOMER_HOJIN_FIELD))
                .put(CUSTOMER_BUSHO_FIELD, customer.get(CUSTOMER_BUSHO_FIELD))
                .put(CUSTOMER_POSTAL_FIELD, customer.get(CUSTOMER_POSTAL_FIELD))
                .put(CUSTOMER_PREF_ID_FIELD, customer.get(CUSTOMER_PREF_ID_FIELD))
                .put(CUSTOMER_PREF_NAME_FIELD, customer.get(CUSTOMER_PREF_NAME_FIELD))
                .put(CUSTOMER_ADDRESS1_FIELD, customer.get(CUSTOMER_ADDRESS1_FIELD))
                .put(CUSTOMER_ADDRESS2_FIELD, customer.get(CUSTOMER_ADDRESS2_FIELD))
                .put(CUSTOMER_MAIL_FIELD, customer.get(CUSTOMER_MAIL_FIELD))
                .put(CUSTOMER_TEL_FIELD, customer.get(CUSTOMER_TEL_FIELD))
                .put(CUSTOMER_FAX_FIELD, customer.get(CUSTOMER_FAX_FIELD))
                .put(CUSTOMER_TEL_MOBILE_FIELD, customer.get(CUSTOMER_TEL_MOBILE_FIELD))
                .put(CUSTOMER_MEMO_FIELD, customer.get(CUSTOMER_MEMO_FIELD))
                .put(CUSTOMER_MEMBER_FIELD, customer.get(CUSTOMER_MEMBER_FIELD))
                .put(CUSTOMER_SALES_COUNT_FIELD, customer.get(CUSTOMER_SALES_COUNT_FIELD))
                .put(CUSTOMER_POINTS_FIELD, customer.get(CUSTOMER_POINTS_FIELD));

        Object sex = customer.get(CUSTOMER_SEX_FIELD);
        if (!sex.equals(null)) {
            customerValue.put(CUSTOMER_SEX_FIELD, sex);
        }

        return customerValue;
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
