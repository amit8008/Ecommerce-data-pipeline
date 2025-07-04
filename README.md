# 🛒 E-Commerce Data Engineering Pipeline



This project demonstrates a real-world data engineering pipeline for an e-commerce platform, including both \*\*batch and streaming\*\* data processing using \*\*Apache Spark\*\*, \*\*Apache Kafka\*\*, \*\*Apache Airflow\*\*, and \*\*Apache Iceberg\*\*, orchestrated on \*\*GCP/AWS\*\* (or simulated locally with Docker).



---



## 🚀 Project Overview



### 🎯 Goals:

* Ingest customer data from PostgreSQL (batch)
* Ingest real-time order events from Kafka (streaming)
* Apply Slowly Changing Dimension (SCD Type 2) to customer data
* Perform near real-time transformations on orders
* Store final data in Apache Iceberg for analytics
* Orchestrate jobs using Apache Airflow
* Analyze key business metrics like LTV, daily revenue





## 🧱 Architecture Diagram



!\[Architecture](docs/architecture.png)







## ⚙️ Tech Stack



| Layer           | Tools/Tech Used                                  |

| ---------------- | --------------------------------------------------- |

| Ingestion       | PostgreSQL, Kafka                                 |

| Processing      | Apache Spark (Scala), Structured Streaming        |

| Storage         | Apache Iceberg / Delta Lake, S3 or GCS (simulated)|

| Orchestration   | Apache Airflow                                    |

| Monitoring      | Logging, Retry Mechanism in Airflow               |

| Analytics       | SQL (Iceberg/BigQuery), Optional: Tableau/Looker  |







## 🛠️ Project Structure



ecommerce-data-pipeline/

│

├── airflow/ # Airflow DAGs and config

├── spark-apps/

│ ├── scala/ # Spark Scala codebase (SBT project)

│ └── notebooks/ # Optional analysis or exploratory notebooks

├── configs/ # Kafka, Iceberg schema, application.conf

├── data/ # Raw, processed, archived data (simulated)

├── docker/ # Docker setup for local testing

├── docs/ # Architecture, diagrams, tech notes

└── scripts/ # Startup scripts, job triggers









## 🔄 Data Flow Summary



1\. **Customer Batch Load**:

   - Source: PostgreSQL

   - Processed using Spark (SCD Type 2 logic)

   - Written to Iceberg in Parquet format



2\. **Order Stream Processing**:

   - Source: Kafka (JSON events)

   - Spark Streaming with watermarking + deduplication

   - Written to Iceberg with upserts



3\. **Orchestration**:

   - Airflow DAGs schedule batch jobs and monitor stream jobs

   - Retry logic, alerts (optional), and lineage tracking



---



## 🧪 How to Run Locally



### 📌 Prerequisites

* Docker \& Docker Compose
* Java 8/11, Scala
* sbt (Scala build tool)
* Python 3.8+ with `venv` or `conda`



### 🐳 Start Services

```bash

cd docker/

docker-compose up -d
```


### 🚀 Run Spark Jobs


```bash

cd spark-apps/scala/

sbt run           # or submit with spark-submit

```



### 🛫 Trigger Airflow DAGs

```bash

cd airflow/

docker-compose up airflow-webserver airflow-scheduler

\\# Access: http://localhost:8080

```



### 📊 Sample Analytics Output





| Metric               | Description                         |

| -------------------- | ----------------------------------- |

| Daily Sales          | Aggregated from streaming orders    |

| Top Customers by LTV | Based on historical + recent orders |

| Repeat Rate          | % of users placing >1 order         |





### 📝 Key Learnings

* Building streaming and batch pipelines in real-world architecture
* Handling CDC and SCD using Spark and Iceberg
* DAG orchestration and recovery with Airflow
* Working with open table formats like Apache Iceberg
* Simulating cloud-based deployment on local Docker



### 📂 Resources

* Airflow Official Docs
* Spark Structured Streaming Guide
* Apache Iceberg Quickstart



### 📧 Contact

For questions, feel free to reach out at amit8singh008@gmail.com or connect on LinkedIn



```yaml


\\## 💡 Bonus Tips:

\\- Add \\\*\\\*GIF demo or screenshots\\\*\\\* of your DAGs and streaming output

\\- Host architecture diagram in `docs/architecture.png`

\\- Consider adding a `Makefile` or `run\\\_all.sh` script for easy testing



Would you like me to generate the diagram (`docs/architecture.png`) or starter Airflow DAG next?

```



