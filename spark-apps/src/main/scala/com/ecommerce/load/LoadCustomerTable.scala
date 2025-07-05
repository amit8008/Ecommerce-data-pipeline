package com.ecommerce.load

import com.ecommerce.utility.IcebergSparkConfig
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, lit}

import java.io.File

object LoadCustomerTable extends App {

  // using ConfigFactory.parseFile(...) to pass application.conf files as argument to run same with java command
  val config = ConfigFactory.parseFile(new File(args(0))).resolve()

  val icebergConf = IcebergSparkConfig(config)

  val spark = SparkSession
    .builder()
    .appName("Load Customer postgresSQL table into Iceberg table")
    .master("local[*]")
    .config(icebergConf)
    .getOrCreate()


  val customerDf = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/ecommerce")
    .option("dbtable", "public.customer")
    .option("user", "amitsingh")
    .option("password", "amitsingh123")
    .load()


// Add metadata columns for SCD Type 2
  val customerSCD2 = customerDf
    .withColumn("start_date", current_date())
    .withColumn("end_date", lit(null).cast("date"))
    .withColumn("is_active", lit(true))


  //  Perform SCD Type 2 merge into Iceberg table


  customerSCD2.createOrReplaceTempView("staged")
  spark.sql(
    """
      |MERGE INTO prod01.ecommerce.customers as target
      |USING staged AS source
      |ON target.customer_id = source.customer_id AND target.is_active = true
      |WHEN MATCHED AND (
      |target.customer_name <> source.customer_name OR
      |target.customer_email <> source.customer_email OR
      |target.customer_address <> source.customer_address
      |)
      |THEN
      |UPDATE SET
      |target.is_active = false,
      |target.end_date = source.start_date
      |WHEN NOT MATCHED THEN
      |INSERT *
      |""".stripMargin)


  //  customerSCD2.writeTo("prod01.ecommerce.customers")
  //    .append()

}
