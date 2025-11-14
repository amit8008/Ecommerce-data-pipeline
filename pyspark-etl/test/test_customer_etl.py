from chispa import assert_df_equality
from pyspark import Row
from pyspark.sql import SparkSession

from jobs.customer_etl import transform


def test_transformed_should_filter_invalid_row_and_add_column(spark: SparkSession):

    input_data = [
        Row(customer_id = 1, name = "Amit", age = 30, country = "India"),
        Row(customer_id = None, name = "PAmit", age = 23, country = "France"),
        Row(customer_id = 2, name = "KAmit", age = 20, country = None),
        Row(customer_id = 3, name = "CAmit", age = 13, country = "Germany"),
    ]

    df_input = spark.createDataFrame(input_data)

    expected_data = [
        Row(customer_id = 1, name = "Amit", age = 30,  country = "India", is_adult = True),
        Row(customer_id = 2, name = "KAmit", age = 20, country = "Unknown", is_adult = True)
    ]

    expected_df = spark.createDataFrame(expected_data)

    df_result = transform(df_input)

    # Assertion
    assert_df_equality(df_result, expected_df,  ignore_nullable = True, ignore_row_order = True)

