from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def extract(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Extract raw csv file from input path
    :param spark: SparkSession
    :param input_path: path to load file
    :return: Dataframe
    """

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )


def transform(df: DataFrame) -> DataFrame:
    """
    Transform dataframe by cleaning null rows, and managing country null with 'unknown'.
    Filter adult customers
    :param df: input dataframe
    :return: Transformed dataframe
    """

    transformed_df = (
        df.dropna(subset = ["customer_id", "age"])
        .filter(F.col("age") >= 18)
        .withColumn("country", F.when(F.col("country").isNull(), F.lit("Unknown")).otherwise(F.col("country"))
                    )
        .withColumn("is_adult", F.lit(True))
    )
    return transformed_df


def load(df: DataFrame, output_path):
    """
    Load data into output csv file
    :param df: Dataframe to load
    :param output_path: path to load the file
    :return: None
    """
    df.write.mode("overwrite").parquet(output_path)


def run_pipeline(spark:SparkSession, input_path, output_path):
    """
    Orchestrate ETL pipeline

    :param spark: SparkSession
    :param input_path: raw csv file
    :param output_path: transformed csv file
    :return: None
    """

    df = extract(spark, input_path)
    df_transformed = transform(df)
    load(df_transformed,output_path)


if __name__ == "__main__":
    print("Running ETL...")
    from utils.spark_utils import get_spark_session
    spark = get_spark_session("Customer ETL local")
    run_pipeline(spark,
                 "/C:/Users/Public/Documents/Ecommerce-data-pipeline/pyspark-etl/data/customer_sample.csv",
                 "/C:/Users/Public/Documents/Ecommerce-data-pipeline/pyspark-etl/output")

