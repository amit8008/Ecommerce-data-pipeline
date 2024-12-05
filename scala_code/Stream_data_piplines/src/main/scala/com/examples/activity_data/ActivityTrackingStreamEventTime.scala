package com.examples.activity_data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger

object ActivityTrackingStreamEventTime extends App {

  val spark = SparkSession
    .builder()
    .appName("Example of Event-Time based stream processing of Activity tracking data")
    .master("local[*]")
    .config("spark.sql.shuffle.partition","5")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  /**
   * Data description: The Heterogeneity Human Activity Recognition Dataset.
   * The data consists of smartphone and smartwatch sensor readings from a variety of devices—specifically,
   * the accelerometer and gyroscope, sampled at the highest possible frequency supported by the devices.
   * Readings from these sensors were recorded while users performed activities like biking, sitting, standing, walking, and so on.
   * There are several different smartphones and smartwatches used, and nine total users.
   * You can download the data here, in the activity data folder.
   * Creation_Time - unix time in nano seconds, Hence divided by 1,000,000,000
   * Arrival_Time - unix time in milli seconds, Hence divided by 1000
   */

  val config = ConfigFactory.load()
  val static = spark.read.json(config.getString("activity_data.path"))
  static.printSchema()

  static
    .select(col("Creation_Time"), col("Arrival_Time"), col("User"))
    .withColumn("Creation_timestamp", (col("Creation_Time")/1e9).cast("timestamp"))
    .withColumn("Arrival_timestamp", to_timestamp(from_unixtime(col("Arrival_Time")/1000)))
    .where(col("User") === "g")
    .show(300, truncate = false)
//      .printSchema()


  val streaming = spark.readStream
    .option("maxFilesPerTrigger", "1")
    .json(config.getString("activity_data.path"))

  streaming.printSchema()

  val withEventTime = streaming
    .withColumn("event_time", (col("Creation_Time")/1e9).cast("timestamp"))

  /*
  Tumbling Windows
   */
//  val eventsPerWindowTumbling =
//    withEventTime
//      .groupBy(window(col("event_time"), "10 minutes"), col("User")).count()
//      .where(col("User") === "g")
//      .orderBy(col("window.start"))
//      .writeStream
////    .queryName("events_per_window_tumbling")
//    .format("console")
//    .option("truncate", "false") // to show complete trigger output
//    .outputMode("complete")
//    .start()
//
//  eventsPerWindowTumbling.awaitTermination()

  /*
  Sliding Windows
   */
  val eventsPerWindowSliding =
    withEventTime
      .groupBy(window(col("event_time"), "10 minutes", "5 minutes"), col("User"))
      .count()
      .where(col("User") === "g")
      .orderBy(col("window.start"))
      .writeStream
      //    .queryName("events_per_window_sliding")
      .format("console")
      .option("truncate", "false") // to show complete trigger output
      .outputMode("complete")
      .start()

  eventsPerWindowSliding.awaitTermination()

}
