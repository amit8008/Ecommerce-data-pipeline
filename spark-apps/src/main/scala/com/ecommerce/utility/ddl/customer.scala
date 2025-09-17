package com.ecommerce.utility.ddl

object customer {
  val customerBronzeTabName = "prod01.ecommerce.customers_bronze"

  val createCustomerBronze = s"""
        |CREATE TABLE IF NOT EXISTS $customerBronzeTabName (
        |customer_id INT,
        |customer_name STRING,
        |customer_phone STRING,
        |customer_email STRING,
        |customer_dob DATE,
        |customer_address STRING,
        |start_date DATE,
        |end_date DATE,
        |is_active BOOLEAN
        |)
        |USING iceberg
        |PARTITIONED BY (bucket(4, customer_id), year(customer_dob));
        |""".stripMargin

  val dropCustomerBronze = s"DROP TABLE IF EXISTS $customerBronzeTabName"

  def selectCustomerBronze(limit: Int = 10) = s"SELECT * FROM $customerBronzeTabName LIMIT $limit"



}
