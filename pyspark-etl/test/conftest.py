import pytest
from pyspark.sql import SparkSession
import os

@pytest.fixture(scope="session")
def spark():
    import os
    os.environ["PYSPARK_PYTHON"] = r"C:\Users\Public\Documents\Ecommerce-data-pipeline\pyspark-etl\.venv1\Scripts\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]

    spark = (
        SparkSession.builder
        .appName("pytest-spark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark
    spark.stop()

