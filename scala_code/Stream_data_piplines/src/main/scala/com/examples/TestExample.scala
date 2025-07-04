package com.examples

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

object TestExample extends App {

  val spark = SparkSession
    .builder
    .appName("Example 1 to check spark is running fine")
    .config("spark.sql.shuffle.partitions", "5")
    .master("local[*]")
    .getOrCreate()

  val config = ConfigFactory.load()


  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("C:\\Users\\Public\\Documents\\Spark\\Spark-The-Definitive-Guide\\data\\flight-data\\csv\\2010-summary.csv")

  flightData2015.select(max(col("count"))).show()
  flightData2015.show(5)


  //Hold the run for 5 mins to check Web UI
  // Thread.sleep(300000)
}
