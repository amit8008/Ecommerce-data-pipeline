from pyspark.sql import SparkSession


def get_spark_session(app_name:str = "LocalPySparkApp") -> SparkSession:
    """
    Get a spark session for ETL application
    :param app_name: Name of spark application
    :return: SparkSession
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partition", "10")
        .getOrCreate()
    )

    return spark

