package com.examples.activity_data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

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
  val config = ConfigFactory.load()
  val static = spark.read.json(config.getString("activity_data.path"))
//  val dataSchema = static.schema
//  println(dataSchema)

  val streaming = spark.readStream
    .option("maxFilesPerTrigger", "1")
    .json(config.getString("activity_data.path"))

  streaming.printSchema()

  val activityCounts = streaming
    .groupBy("gt")
    .count()

//  val activityQuery = activityCounts.writeStream
////    .queryName("activity_counts")
//    .format("console")
//    .outputMode("complete")
//    .start()

  /**
   * Exception in thread "main" org.apache.spark.sql.AnalysisException: Append output mode not supported
   * when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
   */

  /*
  *Transformation
  * Processing time trigger
   */

  import org.apache.spark.sql.functions.expr
  val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
    .where("stairs")
    .where("gt is not null")
    .select("gt", "model", "arrival_time", "creation_time")
    .writeStream
//    .trigger(Trigger.ProcessingTime("30 seconds"))
//    .trigger(Trigger.Once())
    .trigger(Trigger.AvailableNow())
//    .queryName("simple_transform")
    .format("console")
    .outputMode("append")
    .start()

  /*
  *Aggregation
   */
//  val deviceModelStats = streaming.cube("gt", "model").avg()
//    .drop("avg(Arrival_time)")
//    .drop("avg(Creation_Time)")
//    .drop("avg(Index)")
//    .writeStream
////    .queryName("device_counts")
//    .format("console")
//    .outputMode("complete")
//    .start()


  /*
  *Join
   */

//  val historicalAgg = static.groupBy("gt", "model").avg()
//  val deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")
//    .cube("gt", "model").avg()
//    .join(historicalAgg, Seq("gt", "model"))
//    .writeStream
////    .queryName("device_counts")
//    .format("console")
//    .outputMode("complete")
//    .start()


//  deviceModelStats.awaitTermination()
  simpleTransform.awaitTermination()
//  activityQuery.awaitTermination()
}
