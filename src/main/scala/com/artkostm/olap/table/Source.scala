package com.artkostm.olap.table

import com.artkostm.olap.table.reader.{JdbcTableReader, TableReader}
import com.artkostm.olap.table.writer.{JdbcTableWriter, TableWriter}
import org.apache.spark.sql.SparkSession

trait Source {
  def spark: SparkSession
  def reader: TableReader
  def writer: TableWriter
}

trait JdbcSource extends Source {
  def jdbcUrl: String
  def reader: TableReader = new JdbcTableReader(spark, jdbcUrl)
  def writer: TableWriter = new JdbcTableWriter(jdbcUrl)
}

// for testing only
trait CsvSource extends Source {}
