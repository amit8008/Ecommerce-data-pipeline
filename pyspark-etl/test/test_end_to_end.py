import os.path
from pyspark.sql import SparkSession
from jobs.customer_etl import run_pipeline


def test_end_to_end_pipeline(spark, tmp_path) :
    # setup paths
    input_path = os.path.join("/C:/Users/Public/Documents/Ecommerce-data-pipeline/pyspark-etl/data", "customer_sample.csv")
    output_path = tmp_path / "output"

    # run pipeline
    run_pipeline(spark, input_path, str(output_path))

    # Validate output
    df_output = spark.read.parquet(str(output_path))
    result = df_output.select("customer_id").rdd.flatMap(lambda x :x).collect()

    assert 2 not in result
    assert 1 in result
    assert df_output.filter("country = 'India'").count() == 1


