package com.examples.activity_data

import org.apache.spark.sql.SparkSession

object ActivityTrackingStream extends App {

  val spark = SparkSession
    .builder()
    .appName("This is Example of spark steaming for activity tracking data")
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
   */
  val dataPath = "C:\\Users\\Public\\Documents\\Spark\\Spark-The-Definitive-Guide\\data\\activity-data"
//  val static = spark.read.json(dataPath)
//  val dataSchema = static.schema
//  println(dataSchema)

  val streaming = spark.readStream
    .option("maxFilesPerTrigger", "1")
    .json(dataPath)

  val activityCounts = streaming
    .groupBy("gt")
    .count()

  val activityQuery = activityCounts.writeStream
//    .queryName("activity_counts")
    .format("console")
    .outputMode("complete")
    .start()

  activityQuery.awaitTermination()
}
