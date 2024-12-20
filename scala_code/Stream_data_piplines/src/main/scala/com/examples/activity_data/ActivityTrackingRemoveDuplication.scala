package com.examples.activity_data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ActivityTrackingRemoveDuplication extends App {

  val config = ConfigFactory.load()

  val spark = SparkSession
    .builder()
    .appName("This is example of Removing Duplicate data from stream")
    .master("local[*]")
    .config("spark.sql.shuffle.partition", "5")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("spark.sql.streaming.checkpointLocation",config.getString("activity_data.checkpointLocation"))
    .getOrCreate()

  /**
   * The goal here will be to de-duplicate the number of events per user by removing duplicate events.
   * Notice how you need to specify the event time column as a duplicate column along with the column you should de-duplicate.
   * The core assumption is that duplicate events will have the same timestamp as well as identifier.
   */

  val streaming = spark.readStream
    .option("maxFilesPerTrigger", "1")
    .json(config.getString("activity_data.path"))

  val withEvenTime = streaming
    .withColumn("event_time", (col("Creation_Time")/1e9).cast("timestamp"))

  val dropDuplicationFromEvenTime = withEvenTime
    .withWatermark("event_time", "30 minutes")
//    .dropDuplicates("User", "event_time")
    .groupBy("User")
    .count()
    .where(col("User") === "g")
    .writeStream
//    .queryName("deduplicated")
    .format("Console")
    .option("truncate", "false")
    .outputMode("complete")
    .start()

  dropDuplicationFromEvenTime.awaitTermination()

}
