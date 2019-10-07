package com.artkostm.olap.table.fact

import com.artkostm.olap.loader.LoadingStrategy
import com.artkostm.olap.table.{MergeableTable, Rule}
import com.artkostm.olap.table.dim.DimensionTable
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

trait FactTable[T] extends MergeableTable {
  private val logger  = LoggerFactory.getLogger(getClass)
  def sourceDataPath: String

  protected def dimensions: Seq[DimensionTable]

  protected def loadingStrategy: LoadingStrategy

  def update(opts: Map[String, String], params: Map[String, String]): Long = {
    val sourceDf = loadingStrategy.load(sourceDataPath, params).cache()

    logger.info("Updating dimensions tables to ...")
    dimensions.filterNot(_.isStatic).foreach(_.merge(sourceDf, opts, params))
    logger.info("...done")

    logger.info("Reading dimensions from the DB...")
    val dimDfWithDimension = dimensions.map(d => (d.reader.read(d.name, opts, params), d))
    logger.info("...done")

    val transformed = transformations.foldLeft(sourceDf) { case (df, rule) => rule(df) }

    val withDimDataDf = dimDfWithDimension.foldLeft(transformed) {
      case (df, (dimDf, dimension)) => dimension.joinRule(df, dimDf)
    }

    val fact = aggregations.foldLeft(withDimDataDf) { case (df, rule) => rule(df) }.cache()

    logger.info("Writing facts to " + name + "...")
    merge(fact, opts, params)
    logger.info("...done")

    fact.count()
  }

  protected def aggregations: Seq[Rule] = Seq.empty[Rule]

  override protected[table] def merge(source: DataFrame, opts: Map[String, String], params: Map[String, String]): Unit =
    writer.write(source, name, opts = opts, params = params)
}
