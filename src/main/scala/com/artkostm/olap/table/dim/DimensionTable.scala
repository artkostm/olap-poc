package com.artkostm.olap.table.dim

import com.artkostm.olap.table.{MergeableTable, Rule, Source}
import com.artkostm.olap.table.extractor.TableDataExtractor
import com.artkostm.olap.table.reader.TableReader
import com.artkostm.olap.table.writer.TableWriter
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

protected[table] sealed trait DimensionTable extends MergeableTable {
  protected[table] def joinRule: Rule
  protected[table] def isStatic: Boolean
}

protected trait StaticDimensionTable extends DimensionTable {
  override protected[table] def isStatic: Boolean = true
  override protected[table] def merge(
    source: DataFrame,
    opts: Map[String, String],
    params: Map[String, String]
  ): Unit = ()
}

protected trait RegularDimensionTable extends DimensionTable {
  private val LOGGER  = LoggerFactory.getLogger(getClass)
  protected[table] def extractor: TableDataExtractor
  protected[table] def mergeCondition: Column

  override protected[table] def isStatic: Boolean = false

  protected[table] def merge(source: DataFrame, opts: Map[String, String], params: Map[String, String]): Unit = {
    val newDf = extractor.extract(source).as("new")
    val oldDf = reader.read(name, params = params).as("old")

    val dimDataToAppendDf = newDf.join(oldDf, mergeCondition, "leftanti")

    LOGGER.info("Writing to " + name + "...")
    writer.write(dimDataToAppendDf, name, opts = Map.empty[String, String], params = params)
    LOGGER.info("...done")
  }
}

object DimensionTable {

  // constructor for regular dimension tables
  def apply(tableName: String, joinTableRule: Rule, ext: TableDataExtractor, mergeCond: Column)(
    implicit source: Source
  ): RegularDimensionTable =
    new RegularDimensionTable {
      override def mergeCondition: Column        = mergeCond
      override def reader: TableReader           = source.reader
      override def writer: TableWriter           = source.writer
      override def extractor: TableDataExtractor = ext
      override def name: String                  = tableName
      override def joinRule: Rule                = joinTableRule
    }

  // constructor for static dimension tables such as DayDim or AgeGroupDim
  def apply(tableName: String, joinTableRule: Rule)(implicit source: Source): StaticDimensionTable =
    new StaticDimensionTable {
      override def reader: TableReader = source.reader
      override def writer: TableWriter = source.writer
      override def name: String        = tableName
      override def joinRule: Rule      = joinTableRule
    }
}
