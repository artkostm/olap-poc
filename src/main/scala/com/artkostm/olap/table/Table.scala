package com.artkostm.olap.table

import com.artkostm.olap.table.reader.TableReader
import com.artkostm.olap.table.writer.TableWriter
import org.apache.spark.sql.DataFrame

sealed trait Table {
  def name: String
  protected[table] def transformations: Seq[Rule] = Seq.empty[Rule]
}

trait TableOps {
  protected[table] def reader: TableReader
  protected[table] def writer: TableWriter
}

trait MergeableTable extends Table with TableOps {
  protected[table] def merge(source: DataFrame, opts: Map[String, String], params: Map[String, String]): Unit
}
