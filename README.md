# Ecommerce-data-pipeline

#### Project Structure

```

ecommerce-data-pipeline/

│

├── README.md

├── .gitignore

│

├── airflow/

│   ├── dags/

│   │   ├── customer\_ingestion\_dag.py

│   │   ├── order\_stream\_dag.py

│   │   └── cdc\_merge\_dag.py

│   ├── plugins/

│   ├── config/

│   │   └── airflow.cfg (if needed)

│   └── docker/

│       └── docker-compose.yaml  # for local setup

│

├── spark-apps/

│   ├── scala/

│   │   ├── build.sbt

│   │   └── src/

│   │       └── main/

│   │           └── scala/

│   │               ├── com/

│   │               │   └── project/

│   │               │       ├── BatchCustomerJob.scala

│   │               │       ├── OrderStreamJob.scala

│   │               │       └── CDCProcessor.scala

│   │               └── utils/

│   │                   └── SparkSessionFactory.scala

│   └── notebooks/

│       └── analysis\_notebook.ipynb

│

├── configs/

│   ├── kafka/

│   │   └── producer-config.json

│   ├── iceberg/

│   │   └── table-definitions.sql

│   ├── application.conf  # spark or custom config

│   └── schema/

│       └── customer\_schema.avsc

│

├── data/

│   ├── raw/

│   ├── processed/

│   └── archive/

│

├── docker/

│   ├── docker-compose.kafka.yaml

│   ├── Dockerfile.spark

│   └── Dockerfile.airflow

│

├── docs/

│   ├── architecture.png

│   ├── flow-diagram.drawio

│   └── tech\_stack.md

│

└── scripts/

&nbsp;   ├── run\_spark.sh

&nbsp;   ├── start\_services.sh

&nbsp;   └── populate\_data.py 

```



#### Project Setup

PyCharm for dag creations- 

IntelliJ for spark application development - 

