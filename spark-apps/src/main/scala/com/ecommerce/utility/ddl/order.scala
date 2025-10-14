package com.ecommerce.utility.ddl

object order {

  val orderBronzeTabName = "prod01.ecommerce.order_bronze"

  val createOrderBronze: String = s"""
                                |CREATE TABLE IF NOT EXISTS $orderBronzeTabName (
                                |order_id INT,
                                |product_id INT,
                                |customer_id INT,
                                |order_time TIMESTAMP,
                                |source_file STRING
                                |)
                                |USING iceberg
                                |PARTITIONED BY (month(order_time));
                                |""".stripMargin

  val dropOrderBronze = s"DROP TABLE IF EXISTS $orderBronzeTabName"

  def selectOrderBronze(limit: Int = 10) = s"SELECT * FROM $orderBronzeTabName LIMIT $limit"
}
