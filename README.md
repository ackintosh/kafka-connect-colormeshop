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


The [docker-compose.yml](docker-compose.yml) that is included in this repository is based on the Confluent Platform Docker
images. Take a look at the [quickstart](http://docs.confluent.io/3.0.1/cp-docker-images/docs/quickstart.html#getting-started-with-docker-client)
for the Docker images. 

The hostname `confluent` must be resolvable by your host. You will need to determine the ip address of your docker-machine using `docker-machine ip confluent` 
and add this to your `/etc/hosts` file. For example if `docker-machine ip confluent` returns `192.168.99.100` add this:

```
192.168.99.100  confluent
```


```
docker-compose up -d
```


Start the connector with debugging enabled.
 
```
./bin/debug.sh
```

Start the connector with debugging enabled. This will wait for a debugger to attach.

```
export SUSPEND='y'
./bin/debug.sh
```
