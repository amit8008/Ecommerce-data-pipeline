package com.ecommerce.utility

object EcomPsqlConnection {
  def apply(config: com.typesafe.config.Config): Map[String, String] = Map(
    "url" -> config.getString("Ecommerce.postgresql.url"),
    "user" -> config.getString("Ecommerce.postgresql.user"),
    "password" -> config.getString("Ecommerce.postgresql.password")
  )

}
