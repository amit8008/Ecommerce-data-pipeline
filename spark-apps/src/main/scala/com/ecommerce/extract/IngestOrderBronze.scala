package com.ecommerce.extract

import com.ecommerce.utility.IcebergSparkConfig
import com.ecommerce.utility.ddl.customer.customerBronzeTabName
import com.ecommerce.utility.ddl.order.{createOrderBronze, orderBronzeTabName}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, input_file_name, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

import java.io.File

object IngestOrderBronze extends App {
  val config = ConfigFactory.parseFile(new File(args(0))).resolve()

  val icebergConf = IcebergSparkConfig(config)

  val spark = SparkSession
    .builder()
    .appName("Ingest Order date from landing zone into Iceberg table")
    .master("local[*]")
    .config(icebergConf)
    .getOrCreate()

  import spark.implicits._

  // Create order bronze table if not exists
  spark.sql(createOrderBronze)

  val orderSchema = StructType(Seq(
    StructField("order_id", IntegerType, nullable = true),
    StructField("product_id", IntegerType, nullable = true),
    StructField("customer_id", IntegerType, nullable = true),
    StructField("order_time_string", StringType, nullable = true)
  ))

  val orderStream = spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(orderSchema)
    .load(config.getString("Ecommerce.orders.orderLandingZone"))

  // incremental details
  val orderBronze = orderStream
    .withColumn("order_time", to_timestamp(col("order_time_string"), "yyyy-MM-dd'T'HH:mm:ss"))
    .drop("order_time_string")
    .withColumn("source_file", input_file_name())


  val query = orderBronze.writeStream
    .format("iceberg")
    .option("checkpointLocation", config.getString("Ecommerce.orders.orderCheckPointLocation"))
    .outputMode("append")
    .toTable(orderBronzeTabName)

  query.awaitTermination()

}
