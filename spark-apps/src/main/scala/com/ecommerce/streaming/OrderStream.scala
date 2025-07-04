package com.ecommerce.streaming

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, split}
import org.apache.spark.storage.StorageLevel

import java.io.File

object OrderStream extends App {
  val spark = SparkSession
    .builder()
    .appName("Order stream from Kafka")
    .master("local[*]")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host.docker.internal:9092")
    .option("subscribe", "Ecommerce-order_events")
    .option("startingOffsets", "earliest")
    .option("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .load()

  import spark.implicits._
  // Select the value of the Kafka message and cast it to a string
  val kafkaData = df
    .withColumn("Key_str", col("key").cast("String"))
    .withColumn("value_str", col("value").cast("String"))
    .drop("key", "value")

  val orderDf = kafkaData
    .withColumn("order_id", split(col("value_str"), "\\|").getItem(0))
    .withColumn("prod_id", split(col("value_str"), "\\|").getItem(1))
    .withColumn("cust_id", split(col("value_str"), "\\|").getItem(2))
    .withColumn("order_time", split(col("value_str"), "\\|").getItem(3))
    .select("order_id", "prod_id", "cust_id", "order_time")

//  val kafkaData = df.selectExpr("key", "value") --- give redundant values

//  Count of product_id in each batch
  val productCount = orderDf
    .groupBy(col("prod_id"))
    .count()
    .withColumnRenamed("count", "prod_count")

//  join product count df with product df to get other attributes of product
//  val config = ConfigFactory.load()
  val config = ConfigFactory.parseFile(new File(args(0))).resolve()

  println(args(0))

  val productDf = spark.read
    .option("multiline", "true")
    .json(config.getString("Ecommerce.product"))
    .persist(StorageLevel.MEMORY_AND_DISK)

  val joinedDf = productCount
    .join(productDf, productDf("product_id") === productCount("prod_id"), "left")
    .select("prod_id","prod_count", "product_name", "tags")
    .filter(array_contains(col("tags"), "Premium"))
    .orderBy("prod_count")

  // Print the data to the console
//  val query = joinedDf.writeStream
////    .outputMode("append")
//        .outputMode("complete") // only use when streaming aggregation is apply on dataframe
//    .format("console")
//    .option("truncate", "false")
//    .start()

  val query = orderDf
    .writeStream
    .format("parquet")
    .option("checkpointLocation", config.getString("Ecommerce.orderCheckPointLocation"))
    .option("path", config.getString("Ecommerce.orderFilePath"))
    .start()

  query.awaitTermination()


}
