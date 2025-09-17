package com.ecommerce.utility

import com.ecommerce.utility.ddl.customer.{createCustomerBronze, selectCustomerBronze}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.io.File

object IcebergSQLExecutor extends App {

  val config = ConfigFactory.parseFile(new File(args(0))).resolve()
  val icebergConf = IcebergSparkConfig(config)

  val spark = SparkSession
    .builder()
    .appName("Load Customer postgresSQL table into Iceberg table")
    .master("local[*]")
    .config(icebergConf)
    .getOrCreate()


//  spark.sql(createCustomerBronze)
  spark.sql("select count(*) from prod01.ecommerce.customers_bronze").show()
//  spark.sql(selectCustomerBronze())
//    .show()
  spark.sql("select * from prod01.ecommerce.customers_bronze where customer_id = 7534931").show(false)

}
