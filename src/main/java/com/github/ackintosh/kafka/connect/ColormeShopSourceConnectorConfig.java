package com.github.ackintosh.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class ColormeShopSourceConnectorConfig extends AbstractConfig {

    public static final String ACCESS_TOKEN_CONFIG = "access_token";
    private static final String ACCESS_TOKEN_DOC = "Access token";

    public ColormeShopSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public ColormeShopSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef().define(ACCESS_TOKEN_CONFIG, Type.STRING, Importance.HIGH, ACCESS_TOKEN_DOC);
    }

    public String getAccessToken(){
        return this.getString(ACCESS_TOKEN_CONFIG);
    }
}
