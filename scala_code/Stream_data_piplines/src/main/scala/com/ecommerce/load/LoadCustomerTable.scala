package com.ecommerce.load

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

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

  customerDf.show()
  customerDf.writeTo("catalog001.ecommerce.customers").create()

}
