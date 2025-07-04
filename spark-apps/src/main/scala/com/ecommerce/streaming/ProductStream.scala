package com.ecommerce.streaming

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object ProductStream extends App {

  val spark = SparkSession
    .builder()
    .appName("Order stream from Kafka")
    .master("local[*]")
    .getOrCreate()

  val config = ConfigFactory.load()

//  val productDf = spark.read
//    .option("multiline", "true")
//    .json(config.getString("Ecommerce.product"))
//
//  productDf.show()

//  Reading of written order into Spark
  val fileBasedOrder = spark.read
    .load("C:\\Users\\Public\\Documents\\Stream-data-pipelines\\scala_code\\Stream_data_piplines\\src\\main\\resources\\order_file_sink")
    .na.drop(how = "any")

//  fileBasedOrder.show(200)

  fileBasedOrder
    .groupBy("cust_id", "prod_id")
    .count()
    .orderBy(desc("count"))
    .show()


}
