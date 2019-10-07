package com.artkostm.olap.loader

import com.artkostm.olap.JdbcUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.reflect.ClassTag
import scala.util.matching.Regex.Groups

trait LoadingStrategy {
  def load[T <: Product: ClassTag](root: String, props: Map[String, String]): DataFrame
}

object LoadingStrategy {
  val HistoryTable     = "dbo.ingest_data_history"
  val FullUserInfoEtl  = "FullUserInfo"
  val ReportDataSource = "Report"
  val ModelDataSource  = "Model"

  val startDateWindowSpec = Window.partitionBy("year", "month", "day").orderBy(col("hour").desc, col("start").desc)

  def loadSuccessRunHistory(spark: SparkSession,
                            jdbcUrl: String,
                            historyTable: String,
                            dataSource: String,
                            etl: String): DataFrame =
    JdbcUtils
      .readTable(spark, jdbcUrl, historyTable)
      .filter(
        (col("status") === "SUCCESS") &&
        (col("datasource_name") === dataSource) && col("etl_name") === etl
      )
      .withColumn("year", regexp_extract(col("data_path"), "year=(\\d+)\\S", 1))
      .withColumn("month", regexp_extract(col("data_path"), "month=(\\d+)\\S", 1))
      .withColumn("day", regexp_extract(col("data_path"), "day=(\\d+)\\S", 1))
      .withColumn("hour", regexp_extract(col("data_path"), "hour=(\\d+)\\S", 1))
      .withColumn("last_hour", first("hour").over(startDateWindowSpec))
      .withColumn("last_start", first("start").over(startDateWindowSpec))
      .filter(col("start") === col("last_start") && col("hour") === col("last_hour"))
      .withColumnRenamed("run_guid", "etlid")
      .drop("last_hour")
      .cache()
}

case class MissingPartitionsLoadingStrategy(spark: SparkSession,
                                            lastSuccessRunPath: String,
                                            jdbcUrl: String,
                                            historyTable: String,
                                            dsName: String,
                                            etlName: String)
  extends LoadingStrategy {
  override def load[T <: Product: ClassTag](root: String, props: Map[String, String]): DataFrame = {
    val (year, month, day) = extractYearMonthDay(lastSuccessRunPath)
    val history = LoadingStrategy
      .loadSuccessRunHistory(spark, jdbcUrl, historyTable, dsName, etlName)
      .filter(col("year") >= lit(year))
      .filter(col("month") >= lit(month))
      .filter(col("day") > lit(day))
      .cache()

    val partitionedData = spark.read.schema(Encoders.product[T].schema).format("avro").load(root)

    partitionedData.join(history, Seq("year", "month", "day", "hour", "etlid"))
  }

  private def extractYearMonthDay(path: String): (String, String, String) = {
    val pattern = """year=(\d+)\Smonth=(\d+)\Sday=(\d+)\S""".r

    pattern.findAllMatchIn(path).toArray match {
      case Array(Groups(year, month, day)) => (year, month, day)
      case _                               => throw new RuntimeException("Wrong partition path: " + path)
    }
  }
}

case class AllPartitionsLoadingStrategy(spark: SparkSession,
                                        jdbcUrl: String,
                                        historyTable: String,
                                        dsName: String,
                                        etlName: String)
  extends LoadingStrategy {

  override def load[T <: Product: ClassTag](root: String, connectionProps: Map[String, String]): DataFrame = {
    val history = LoadingStrategy
      .loadSuccessRunHistory(spark, jdbcUrl, historyTable, dsName, etlName)
      .cache()

    val partitionedData = spark.read.schema(Encoders.product[T].schema).format("avro").load(root)

    partitionedData.join(history, Seq("year", "month", "day", "hour", "etlid"))
  }
}
