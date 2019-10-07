package com.artkostm.olap

import com.artkostm.olap.loader.LoadingStrategy
import com.artkostm.olap.table.fact.{Fact1Table, Fact2Table}
import org.apache.spark.sql.SparkSession

object StarSchemaPublisher extends App {
  val sparkJdbcOpts = Map("truncate" -> "true")

  val spark = SparkSession.builder().getOrCreate()

  val loadingStrategy: LoadingStrategy = null//any loading strategy

  val fact1 = Fact1Table(sparkSession = spark,
    jdbc = "jdbcUrl",
    sourcePath = "modelPath",
    dataLoadingStrategy = loadingStrategy)

  val fact2 = Fact2Table(sparkSession = spark,
    jdbc = "jdbcUrl",
    sourcePath = "modelPath",
    dataLoadingStrategy = loadingStrategy)

  fact1.update(sparkJdbcOpts, JdbcUtils.MsSqlConnectionParams)
  fact2.update(sparkJdbcOpts, JdbcUtils.MsSqlConnectionParams)
}
