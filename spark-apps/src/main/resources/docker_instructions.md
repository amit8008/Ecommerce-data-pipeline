# Docker setup for Kafka and spark(for spark submit)

## Kafka in docker
### create docker-compse.yml with below contents
```yml
---
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.2
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:7.3.2
    container_name: broker
    ports:
    # To learn about configuring Kafka for access across networks see
    # https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/
      - "9092:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://broker:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1


```

### common commands for kafka consumer and producers 

open kafka broker to run producer and consumer in different terminal
```shell
docker exec -it <container_id> bash
```
list kafka topics  
```shell
cd /bin/
kafka-topics --bootstrap-server=localhost:9092 --list
```
Create kafka-topics
```shell
cd /bin/
kafka-topics --create --topic Ecommerce-seller_events --bootstrap-server localhost:9092 --partitions 1
kafka-topics --create --topic Ecommerce-order_events --bootstrap-server localhost:9092 --partitions 1
```

### python setup for kafka producer
~~pip install kafka-python-ng~~
```shell
pip install confluent-kafka
```

open kafka producer in one terminal
```shell
cd /bin/
kafka-console-producer --bootstrap-server=localhost:9092 --topic my-topic
```
open kafka consumer in other terminal
```shell
cd /bin/
kafka-console-consumer --bootstrap-server=localhost:9092 --topic Ecommerce-seller_events --from-beginning
kafka-console-consumer --bootstrap-server=localhost:9092 --topic Ecommerce-order_events --from-beginning
```

open kafka consumer in other terminal
```shell
cd /bin/
kafka-console-consumer --bootstrap-server localhost:9092 --topic my-topic --formatter kafka.tools.DefaultMessageFormatter --property print.offset=true --property print.timestamp=true --property print.key=true --property print.value=true --from-beginning
```



## Spark in docker

To integrate `spark-defaults.conf` and `log4j.properties` files into the Docker Compose setup, you should mount these configuration files into the appropriate directories inside the Docker containers. Here's how you can do it:

### Step-by-Step Instructions

1. **Create the Configuration Files**:

   Create `spark-defaults.conf` and `log4j.properties` files in a directory on host machine. For example, create a directory named `spark-config` and place the files there.

   ```sh
   mkdir -p ./spark-config
   ```

   Create the `spark-defaults.conf` file:

   ```sh
   touch ./spark-config/spark-defaults.conf
   ```

   Add the following content to `spark-defaults.conf`:

   ```properties
   spark.eventLog.enabled           true
   spark.eventLog.dir               file:/tmp/spark-events
   spark.history.fs.logDirectory    file:/tmp/spark-events
   spark.history.fs.cleaner.enabled true
   ```

   Create the `log4j.properties` file:

   ```sh
   touch ./spark-config/log4j.properties
   ```

   Add the following content to `log4j.properties`:

   ```properties
   log4j.rootCategory=WARN, console
   log4j.appender.console=org.apache.log4j.ConsoleAppender
   log4j.appender.console.target=System.err
   log4j.appender.console.layout=org.apache.log4j.PatternLayout
   log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
   ```

2. **Update the Docker Compose File**:

   Modify the `docker-compose.yml` file to mount these configuration files into the Spark containers.

   ```yaml
   version: '3.8'

   services:
     spark-master:
       image: bitnami/spark:3.3.0
       hostname: spark-master
       container_name: spark-master
       environment:
         - SPARK_MODE=master
         - SPARK_RPC_AUTHENTICATION_ENABLED=no
         - SPARK_RPC_ENCRYPTION_ENABLED=no
         - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
         - SPARK_SSL_ENABLED=no
       ports:
         - "8080:8080"
         - "7077:7077"
       volumes:
         - spark-logs:/opt/spark/logs
         - spark-events:/tmp/spark-events
         - ./spark-config/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
         - ./spark-config/log4j.properties:/opt/bitnami/spark/conf/log4j.properties

     spark-worker:
       image: bitnami/spark:3.3.0
       hostname: spark-worker
       container_name: spark-worker
       environment:
         - SPARK_MODE=worker
         - SPARK_MASTER_URL=spark://spark-master:7077
         - SPARK_WORKER_MEMORY=1G
         - SPARK_WORKER_CORES=1
       ports:
         - "8081:8081"
       depends_on:
         - spark-master
       volumes:
         - spark-logs:/opt/spark/logs
         - spark-events:/tmp/spark-events
         - ./spark-config/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
         - ./spark-config/log4j.properties:/opt/bitnami/spark/conf/log4j.properties

     spark-history-server:
       image: bitnami/spark:3.3.0
       hostname: spark-history-server
       container_name: spark-history-server
       environment:
         - SPARK_MODE=history-server
         - SPARK_HISTORY_SERVER_LOG_DIR=/tmp/spark-events
         - SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=/tmp/spark-events
       ports:
         - "18080:18080"
       depends_on:
         - spark-master
       volumes:
         - spark-logs:/opt/spark/logs
         - spark-events:/tmp/spark-events
         - ./spark-config/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
         - ./spark-config/log4j.properties:/opt/bitnami/spark/conf/log4j.properties

   volumes:
     spark-logs:
       driver: local
     spark-events:
       driver: local
   ```

### Running the Setup

1. **Start the Docker Compose Services**:

   Run the following command in the directory containing `docker-compose.yml` file.

   ```sh
   docker-compose up -d
   ```

2. **Access the Spark UIs**:

    - **Spark Master UI**: http://localhost:8080
    - **Spark Worker UI**: http://localhost:8081
    - **Spark History Server UI**: http://localhost:18080

3. **Submit a Spark Application**:

   You can submit a Spark application using the `spark-submit` command from within the Spark master container.

   ```sh
   docker exec -it spark-master spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi /opt/bitnami/spark/examples/jars/spark-examples_2.12-3.3.0.jar 100
   ```

By following these steps, set up is completed and run Apache Spark along with its history server using Docker Compose, with the necessary configuration files mounted appropriately.