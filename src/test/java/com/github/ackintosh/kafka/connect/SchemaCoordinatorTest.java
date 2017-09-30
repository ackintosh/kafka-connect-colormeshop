package com.github.ackintosh.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class SchemaCoordinatorTest {
    @Test
    public void buildSaleValue() {
        String json = "{\"noshi_total_charge\":0," +
                "\"use_yahoo_points\":0," +
                "\"other_discount\":0," +
                "\"paid_mail_state\":\"not_yet\"," +
                "\"other_discount_name\":\"\"," +
                "\"fee\":0," +
                "\"memo\":\"\"," +
                "\"gmo_point_state\":\"assumed\"," +
                "\"delivered\":false," +
                "\"granted_yahoo_points\":0," +
                "\"wrapping_total_charge\":0," +
                "\"delivery_total_charge\":216," +
                "\"use_gmo_points\":0," +
                "\"card_total_charge\":0," +
                "\"point_state\":\"assumed\"," +
                "\"payment_id\":736549," +
                "\"gmo_point_discount\":0," +
                "\"sale_deliveries\":" +
                "[" +
                "{\"sale_id\":12345678," +
                "\"wrapping_charge\":null," +
                "\"delivery_id\":12345," +
                "\"memo\":\"\"," +
                "\"detail_ids\":[12345678]," +
                "\"delivered\":false," +
                "\"noshi_charge\":null," +
                "\"card_text\":null," +
                "\"preferred_period\":null," +
                "\"wrapping_name\":null," +
                "\"tel\":\"0312345678\"," +
                "\"id\":99999," +
                "\"noshi_text\":null," +
                "\"preferred_date\":null," +
                "\"furigana\":\"テスト　タロウ\"," +
                "\"address2\":\"テストアパート・マンション名\"," +
                "\"address1\":\"テスト区\"," +
                "\"total_charge\":1080," +
                "\"pref_name\":\"東京都\"," +
                "\"account_id\":\"PA12345678\"," +
                "\"delivery_charge\":216," +
                "\"pref_id\":13," +
                "\"name\":\"試験　太郎\"," +
                "\"card_name\":null," +
                "\"card_charge\":null," +
                "\"postal\":\"1000001\"," +
                "\"slip_number\":null}" +
                "]," +
                "\"use_points\":0," +
                "\"accepted_mail_sent_date\":null," +
                "\"details\":" +
                "[" +
                "{\"sale_id\":12345678," +
                "\"subtotal_price\":864," +
                "\"product_mobile_image_url\":null," +
                "\"price_with_tax\":864," +
                "\"product_num\":1," +
                "\"product_image_url\":\"http://img21.shop-pro.jp/PA12345/678/product/117182895.png?cmsp_timestamp=20170525221232\"," +
                "\"product_cost\":500," +
                "\"product_name\":\"象の頭をもった神様\"," +
                "\"option1_index\":0," +
                "\"product_thumbnail_image_url\":\"http://img21.shop-pro.jp/PA12345/678/product/117182895_th.png?cmsp_timestamp=20170525221232\"," +
                "\"unit\":\"個\"," +
                "\"account_id\":\"PA12345678\"," +
                "\"sale_delivery_id\":99999999," +
                "\"option2_index\":0," +
                "\"price\":800," +
                "\"product_id\":117182895," +
                "\"option2_value\":null," +
                "\"id\":189812388," +
                "\"option1_value\":null," +
                "\"product_model_number\":\"KATABAN\"}" +
                "]," +
                "\"id\":987654321," +
                "\"product_total_price\":864," +
                "\"granted_gmo_points\":0," +
                "\"delivered_mail_sent_date\":null," +
                "\"accepted_mail_state\":\"not_yet\"," +
                "\"delivered_mail_state\":\"not_yet\"," +
                "\"total_price\":1080," +
                "\"granted_points\":8," +
                "\"mobile\":false," +
                "\"yahoo_point_state\":\"assumed\"," +
                "\"tax\":64," +
                "\"make_date\":1506384556," +
                "\"update_date\":1506384556," +
                "\"paid_mail_sent_date\":null," +
                "\"canceled\":false," +
                "\"account_id\":\"PA12345678\"," +
                "\"paid\":false," +
                "\"point_discount\":0," +
                "\"customer\":" +
                "{" +
                "\"furigana\":\"テスト　タロウ\"," +
                "\"other\":\"備考\\r\\n<b>備考<\\/b>\\r\\n備考\"," +
                "\"mail\":\"test@example.com\"," +
                "\"hojin\":\"\"," +
                "\"address2\":\"テストアパート・マンション名\"," +
                "\"address1\":\"テスト区\"," +
                "\"tel_mobile\":\"\"," +
                "\"sex\":null," +
                "\"memo\":\"\"," +
                "\"sales_count\":168," +
                "\"pref_name\":\"東京都\"," +
                "\"points\":110," +
                "\"account_id\":\"PA12345678\"," +
                "\"pref_id\":13," +
                "\"name\":\"試験　太郎\"," +
                "\"member\":true," +
                "\"tel\":\"0312345678\"," +
                "\"id\":11111111," +
                "\"postal\":\"1000001\"," +
                "\"busho\":\"\"," +
                "\"fax\":\"0387654321\"" +
                "}" +
                "}";
        Struct saleValue = SchemaCoordinator.buildSaleValue(new JSONObject(json));

        Assert.assertEquals(987654321, (long)saleValue.getInt32("id"));
        Assert.assertEquals(11111111, (long) saleValue.getStruct("customer").getInt32("id"));
    }
}
