package com.artkostm.olap.table.fact

import com.artkostm.olap.loader.LoadingStrategy
import com.artkostm.olap.table.dim.DimensionTable
import com.artkostm.olap.table.{JdbcSource, Rule, Source}
import com.artkostm.olap.table.extractor.{FormalOrgDataExtractor, InformalOrgDataExtractor}
import com.petrofac.data.reporting.loader.LoadingStrategy
import com.petrofac.data.reporting.table.dim.DimensionTable
import com.petrofac.data.reporting.table.extractor.{FormalOrgDataExtractor, InformalOrgDataExtractor}
import com.petrofac.data.reporting.table.{Source, _}
import com.petrofac.data.reporting.{dimension, fact, whitelist}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Fact2Table extends FactTable { self: Source =>
  implicit val source = self

  override def name: String = fact.OrganizationCountFact

  override def transformations: Seq[Rule] = Seq(
    FilterRule(col("is_active") === true),
    FilterRule(lower(col("assignment_status")).isin(whitelist.AssignmentStatuses.map(_.toLowerCase): _*)),
    FilterRule(lower(col("worker_type")).isin(whitelist.WorkerTypes.map(_.toLowerCase): _*)),
    Fact2Table.WithOrgDataColumnsRule
  )

  override protected def dimensions: Seq[DimensionTable] = Seq(
    DimensionTable(dimension.InformalOrg, Fact2Table.JoinInformalOrgRule),
    DimensionTable(dimension.FormalOrg, Fact2Table.JoinFormalOrgRule),
    DimensionTable(dimension.Day, Fact2Table.JoinDayRule)
  )

  override protected def aggregations: Seq[Rule] = Seq(
    Aggregation(Seq("DayKey", "InformalOrgKey", "FormalOrgKey").map(col), count("*").alias("OrganizationCount"))
  )
}

object Fact2Table {
  val JoinInformalOrgRule: Rule = Fact1Table.JoinInformalOrgRule
  val JoinFormalOrgRule: Rule   = Fact1Table.JoinFormalOrgRule
  val JoinDayRule: Rule         = Fact1Table.JoinDayRule

  val WithOrgDataColumnsRule: Rule = new Rule {
    override def apply(df: DataFrame): DataFrame =
      df.select(
        (InformalOrgDataExtractor.InformalOrgCols.keys.toSeq ++
        FormalOrgDataExtractor.FormalOrgCols.keys.toSeq ++ Seq("year", "month", "day"))
          .map(col): _*
      )
  }

  def apply(sparkSession: SparkSession,
            jdbc: String,
            sourcePath: String,
            dataLoadingStrategy: LoadingStrategy): Fact2Table =
    new Fact2Table with JdbcSource {
      override def sourceDataPath: String                     = sourcePath
      override protected def loadingStrategy: LoadingStrategy = dataLoadingStrategy
      override def jdbcUrl: String                            = jdbc
      override def spark: SparkSession                        = sparkSession
    }
}
