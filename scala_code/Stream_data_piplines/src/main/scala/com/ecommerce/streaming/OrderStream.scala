package com.ecommerce.streaming

import org.apache.spark.sql.SparkSession

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
  val kafkaData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  // Print the data to the console
  val query = kafkaData.writeStream
    .outputMode("append")
    //    .outputMode("complete") // only use when streaming aggregation is apply on dataframe
    .format("console")
    .option("truncate", "false")
    .start()

  query.awaitTermination()


}
