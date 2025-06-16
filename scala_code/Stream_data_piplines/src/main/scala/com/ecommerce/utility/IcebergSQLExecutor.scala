package com.ecommerce.utility

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.io.File

object IcebergSQLExecutor extends App {

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


//    spark.sql(
//      """
//        |CREATE TABLE prod01.ecommerce.customers (
//        |customer_id INT,
//        |customer_name STRING,
//        |customer_phone STRING,
//        |customer_email STRING,
//        |customer_dob DATE,
//        |customer_address STRING,
//        |start_date DATE,
//        |end_date DATE,
//        |is_active BOOLEAN
//        |)
//        |USING iceberg
//        |PARTITIONED BY (bucket(4, customer_id), year(customer_dob));
//        |""".stripMargin)

  spark.table("prod01.ecommerce.customers")
    .where(col("customer_name").like("%Dhillon%"))
    .show()


//  spark.sql("drop table prod01.ecommerce.customers")


}
