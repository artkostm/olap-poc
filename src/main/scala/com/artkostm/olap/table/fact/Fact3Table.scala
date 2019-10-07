package com.artkostm.olap.table.fact

import com.artkostm.olap.table.{JdbcSource, Source}
import com.petrofac.data.reporting.loader.LoadingStrategy
import com.petrofac.data.reporting.table._
import com.petrofac.data.reporting.table.dim.DimensionTable
import com.petrofac.data.reporting.{dimension, fact}
import org.apache.spark.sql.SparkSession

trait Fact3Table extends FactTable { self: Source =>
  implicit val source = self

  override def name: String = fact.WorklogCountFact

  override protected def dimensions: Seq[DimensionTable] = Seq(
    DimensionTable(dimension.InformalOrg, Fact3Table.JoinInformalOrgRule),
    DimensionTable(dimension.FormalOrg, Fact3Table.JoinFormalOrgRule),
    DimensionTable(dimension.Day, Fact3Table.JoinDayRule)
  )

  override protected def aggregations: Seq[Rule] = ???

  override protected[table] def transformations: Seq[Rule] = ???
}

object Fact3Table {
  val JoinInformalOrgRule: Rule = Fact1Table.JoinInformalOrgRule
  val JoinFormalOrgRule: Rule   = Fact1Table.JoinFormalOrgRule
  val JoinDayRule: Rule         = Fact1Table.JoinDayRule

  def apply(sparkSession: SparkSession,
            jdbc: String,
            sourcePath: String,
            dataLoadingStrategy: LoadingStrategy): Fact3Table =
    new Fact3Table with JdbcSource {
      override def sourceDataPath: String                     = sourcePath
      override protected def loadingStrategy: LoadingStrategy = dataLoadingStrategy
      override def jdbcUrl: String                            = jdbc
      override def spark: SparkSession                        = sparkSession
    }
}
