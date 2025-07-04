package com.ecommerce.load

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, lit}

import java.io.File

object LoadCustomerTable extends App {
  val config = ConfigFactory.parseFile(new File(args(0))).resolve()

  val spark = SparkSession
    .builder()
    .appName("Load Customer postgresSQL table into Iceberg table")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type ", "hive")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", config.getString("Iceberg.warehouse"))
    .config("spark.sql.defaultCatalog", "local")
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
