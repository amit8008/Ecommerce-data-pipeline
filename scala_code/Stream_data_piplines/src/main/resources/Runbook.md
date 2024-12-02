# Submitting spark application

### test TestExample class with spark submit command
```shell
spark-submit --class TestExample \
Stream_data_pipelines-assembly-0.1.0-SNAPSHOT.jar \
"/opt/bitnami/spark/amit/data/flight-data/csv/2015-summary.csv"
```

### Run kafka consumer with key timestamp
```shell
 kafka-console-consumer --bootstrap-server localhost:9092 --topic my-topic --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --from-beginning
```

### Data pipeline
python_producer -> kafka topic -> spark streaming
