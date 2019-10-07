package com.artkostm.olap.table.writer

import java.io.File
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}

trait TableWriter {

  def write(df: DataFrame,
            destination: String,
            opts: Map[String, String] = Map.empty[String, String],
            params: Map[String, String] = Map.empty[String, String],
            mode: SaveMode = SaveMode.Append)
}

class JdbcTableWriter(jdbcUrl: String) extends TableWriter {
  override def write(df: DataFrame,
                     destTableName: String,
                     opts: Map[String, String],
                     params: Map[String, String],
                     mode: SaveMode): Unit =
    opts
      .foldLeft(df.write.mode(mode)) {
        case (writer, (name, value)) => writer.option(name, value)
      }
      .jdbc(jdbcUrl, destTableName, params.foldLeft(new Properties()) {
        case (props, (name, value)) =>
          props.put(name, value)
          props
      })
}

class CsvTableWriter(tmpDir: String) extends TableWriter {
  override def write(df: DataFrame,
                     destination: String,
                     opts: Map[String, String],
                     params: Map[String, String],
                     mode: SaveMode): Unit =
    opts
      .foldLeft(df.write.mode(mode)) {
        case (writer, (name, value)) => writer.option(name, value)
      }
      .csv(tmpDir + File.separator + destination)
}
