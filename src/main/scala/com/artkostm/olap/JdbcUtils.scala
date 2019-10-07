package com.artkostm.olap

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

// todo: move to common
object JdbcUtils {
  val MsSqlDriverClass      = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val MsSqlConnectionParams = Map("Driver" -> MsSqlDriverClass)

  def readTable(spark: SparkSession,
                jdbcUrl: String,
                table: String,
                options: Map[String, String] = Map.empty[String, String],
                props: Map[String, String] = MsSqlConnectionParams): DataFrame =
    options
      .foldLeft(spark.read) {
        case (reader, (name, value)) => reader.option(name, value)
      }
      .jdbc(jdbcUrl, table, props.foldLeft(new Properties()) {
        case (props, (name, value)) =>
          props.put(name, value)
          props
      })
}
