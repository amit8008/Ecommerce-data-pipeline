# Submitting spark application

## test TestExample class with spark submit command
```shell
spark-submit --class TestExample \
Stream_data_pipelines-assembly-0.1.0-SNAPSHOT.jar \
"/opt/bitnami/spark/amit/data/flight-data/csv/2015-summary.csv"
```

## 