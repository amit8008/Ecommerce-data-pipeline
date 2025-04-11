package com.ecommerce.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object OrderStream extends App {
  val spark = SparkSession
    .builder()
    .appName("Order stream from Kafka")
    .master("local[*]")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Ecommerce-order_events")
    .option("startingOffsets", "earliest")
    .load()

  import spark.implicits._
  // Select the value of the Kafka message and cast it to a string
  val kafkaData = df
    .withColumn("Key_str", col("key").cast("String"))
    .withColumn("value_str", col("value").cast("String"))
    .drop("key", "value")

//  val kafkaData = df.selectExpr("key", "value") --- give redundant values

//  val orderStream = kafk


  // Print the data to the console
  val query = kafkaData.writeStream
    .outputMode("append")
    //    .outputMode("complete") // only use when streaming aggregation is apply on dataframe
    .format("console")
    .option("truncate", "false")
    .start()

  query.awaitTermination()


}
