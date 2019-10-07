package com.artkostm.olap.table

import org.apache.spark.sql.{Column, DataFrame}

trait Rule {
  def apply(df: DataFrame): DataFrame = df
  def apply(left: DataFrame, right: DataFrame) = left
}

case object NoRule extends Rule

case class Aggregation(cols: Seq[Column], expr: Column, exprs: Column*) extends Rule {
  override def apply(df: DataFrame): DataFrame = df.groupBy(cols: _*).agg(expr, exprs: _*)
}

case class FilterRule(condition: Column) extends Rule {
  override def apply(df: DataFrame): DataFrame = df.filter(condition)
}
