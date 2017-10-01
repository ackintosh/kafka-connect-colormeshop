## Kafka Connect ColormeShop

### Overview

This Kafka Connect connector integrates with the [ColormeShop API](https://shop-pro.jp/func/api/) to provide realtime updates to Kafka as objects are created in ColormeShop.

### Configuration

```
name=ColormeShopSourceConnector
connector.class=com.github.ackintosh.kafka.connect.ColormeShopSourceConnector
tasks.max=1

# Set there required values
topic=<The Kafka topic to write the ColormeShop data to.>
access_token=<The ColormeShop API access token.>
```

### Building

```
$ git clone https://github.com/ackintosh/kafka-connect-colormeshop.git
$ cd kafka-connect-colormeshop

# For now, test requires access token.
$ mvn clean package -Daccess_token=<your access token>

# If you won't run tests, 
$ mvn clean package -Dmaven.test.skip=true
```

### Run tests

```
$ mvn test -Daccess_token=<your access token>
```

### Running in development

The [docker-compose.yml](docker-compose.yml) that is included in this repository is based on [Landoop/fast-data-dev](https://github.com/Landoop/fast-data-dev).

```
$ docker-compose up
```

