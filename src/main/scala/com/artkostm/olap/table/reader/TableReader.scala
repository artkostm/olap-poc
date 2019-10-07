package com.artkostm.olap.table.reader

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

trait TableReader {

  def read(target: String,
           opts: Map[String, String] = Map.empty[String, String],
           params: Map[String, String] = Map.empty[String, String]): DataFrame
}

class JdbcTableReader(spark: SparkSession, jdbcUrl: String) extends TableReader {
  override def read(target: String, opts: Map[String, String], params: Map[String, String]): DataFrame =
    opts
      .foldLeft(spark.read) {
        case (reader, (name, value)) => reader.option(name, value)
      }
      .jdbc(jdbcUrl, target, params.foldLeft(new Properties()) {
        case (props, (name, value)) =>
          props.put(name, value)
          props
      })
}
