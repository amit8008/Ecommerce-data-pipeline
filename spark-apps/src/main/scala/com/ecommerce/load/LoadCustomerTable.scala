package com.ecommerce.load

import com.ecommerce.utility.{EcomPsqlConnection, IcebergSparkConfig}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, lit, to_timestamp}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

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


  val ecomPsqlConnection = EcomPsqlConnection(config)
  val customerDf = spark.read
    .format("jdbc")
    .options(ecomPsqlConnection)
    .option("dbtable", config.getString("Ecommerce.postgresql.customer"))
    .load()


//  customerDf.show()

// Add metadata columns for SCD Type 2
  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
  val currentTimestamp = DateTime.now().toString(formatter)
  val customerSCD2 = customerDf
    .withColumn("start_date", to_timestamp(lit(currentTimestamp)))
    .withColumn("end_date", lit(null).cast("date"))
    .withColumn("is_active", lit(true))

//  customerSCD2.show()


  //  Perform SCD Type 2 merge into Iceberg table

  customerSCD2.createOrReplaceTempView("staged")
  spark.sql(
    """
      |MERGE INTO prod01.ecommerce.customers_bronze AS target
      |USING staged AS source
      |ON target.customer_id = source.customer_id AND target.is_active = true
      |WHEN MATCHED AND (
      |    target.customer_name <> source.customer_name OR
      |    target.customer_email <> source.customer_email OR
      |    target.customer_address <> source.customer_address
      |)
      |THEN UPDATE SET
      |    is_active = false,
      |    end_date = source.start_date   -- or current_date
      |
      |WHEN NOT MATCHED BY source AND (target.is_active = true)
      |THEN UPDATE SET
      |    is_active = false,
      |    end_date = current_timestamp ;
      |""".stripMargin)

  spark.sql(
    """
      |INSERT INTO prod01.ecommerce.customers_bronze (
      |    customer_id,
      |    customer_name,
      |    customer_phone,
      |    customer_dob,
      |    customer_email,
      |    customer_address,
      |    is_active,
      |    start_date,
      |    end_date
      |)
      |SELECT
      |    s.customer_id,
      |    s.customer_name,
      |    s.customer_phone,
      |    s.customer_dob,
      |    s.customer_email,
      |    s.customer_address,
      |    true AS is_active,
      |    s.start_date,   -- or current_date
      |    NULL AS end_date
      |FROM staged s
      |LEFT JOIN prod01.ecommerce.customers_bronze t
      |    ON s.customer_id = t.customer_id
      |   AND t.is_active = true
      |WHERE t.customer_id IS NULL            -- new customer
      |   OR t.customer_name <> s.customer_name
      |   OR t.customer_email <> s.customer_email
      |   OR t.customer_address <> s.customer_address;
      |
      |""".stripMargin
  )

  spark.sql("select * from prod01.ecommerce.customers_bronze where customer_id = 9845732").show(false)

}
