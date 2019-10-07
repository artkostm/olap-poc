package com.artkostm.olap.table.fact

import com.artkostm.olap.loader.LoadingStrategy
import com.artkostm.olap.table.{JdbcSource, Source}
import com.artkostm.olap.table.dim.DimensionTable
import com.artkostm.olap.table.extractor.{EmployeeDetailExtractor, FormalOrgDataExtractor, GenderDataExtractor, GradeDataExtractor, InformalOrgDataExtractor, LocationDataExtractor, NationalityDataExtractor, PersonTypeDataExtractor}
import com.petrofac.data.reporting.loader.LoadingStrategy
import com.petrofac.data.reporting.table.dim.DimensionTable
import com.petrofac.data.reporting.table.extractor._
import com.petrofac.data.reporting.table._
import com.petrofac.data.reporting.{ageGroupUdf, dimension, fact, whitelist}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

trait Fact1Table extends FactTable { self: Source =>
  private val LOGGER  = LoggerFactory.getLogger(getClass)
  implicit val source = self

  override def update(opts: Map[String, String], params: Map[String, String]): Long = {
    val sourceDf = loadingStrategy.load(sourceDataPath, params).cache()

    LOGGER.info("Updating dimension tables ...")
    dimensions.filterNot(_.isStatic).foreach(_.merge(sourceDf, opts, params))
    LOGGER.info("...done")

    LOGGER.info("Reading dimensions from the DB...")
    val dimDfWithDimension = dimensions.map(d => (d.reader.read(d.name, opts = opts, params = params), d))
    LOGGER.info("...done")

    val transformed = transformations.foldLeft(sourceDf) { case (df, rule) => rule(df) }

    val withDimDataDf = dimDfWithDimension
      .foldLeft(transformed) {
        case (df, (dimDf, dimension)) => dimension.joinRule(df, dimDf)
      }
      .cache()

    overwriteEmployeeDetail(withDimDataDf, opts, params)

    val fact = aggregations.foldLeft(withDimDataDf) { case (df, rule) => rule(df) }

    LOGGER.info("Writing facts to " + name + "...")
    merge(fact, opts, params)
    LOGGER.info("...done")

    fact.count()
  }

  override def name: String = fact.EmployeeHeadcountFact

  override protected def dimensions: Seq[DimensionTable] = Seq(
    DimensionTable(
      dimension.Location,
      Fact1Table.JoinLocationRule,
      LocationDataExtractor,
      mergeCond = lower(col("old.country")) <=> lower(col("new.country"))
    ),
    DimensionTable(
      dimension.PersonType,
      Fact1Table.JoinPersonTypeRule,
      PersonTypeDataExtractor,
      mergeCond = lower(col("old.name")) <=> lower(col("new.name"))
    ),
    DimensionTable(
      dimension.Gender,
      Fact1Table.JoinGenderRule,
      GenderDataExtractor,
      mergeCond = lower(col("old.name")) <=> lower(col("new.name"))
    ),
    DimensionTable(
      dimension.Grade,
      Fact1Table.JoinGradeRule,
      GradeDataExtractor,
      mergeCond = lower(col("old.name")) <=> lower(col("new.name"))
    ),
    DimensionTable(
      dimension.Nationality,
      Fact1Table.JoinNationalityRule,
      NationalityDataExtractor,
      mergeCond = lower(col("old.name")) <=> lower(col("new.name"))
    ),
    DimensionTable(dimension.AgeGroup, joinTableRule = Fact1Table.JoinAgeGroupRule),
    DimensionTable(dimension.Day, joinTableRule = Fact1Table.JoinDayRule),
    DimensionTable(
      dimension.InformalOrg,
      Fact1Table.JoinInformalOrgRule,
      InformalOrgDataExtractor,
      mergeCond = InformalOrgDataExtractor.InformalOrgCols.values
        .map(
          colName => lower(col("old." + colName)) <=> lower(col("new." + colName))
        )
        .reduce(_ && _)
    ),
    DimensionTable(
      dimension.FormalOrg,
      Fact1Table.JoinFormalOrgRule,
      FormalOrgDataExtractor,
      mergeCond = FormalOrgDataExtractor.FormalOrgCols.values
        .map(
          colName => lower(col("old." + colName)) <=> lower(col("new." + colName))
        )
        .reduce(_ && _)
    )
  )

  override def aggregations: Seq[Rule] = Seq(
    Aggregation(
      Seq(
        "AgeGroupKey",
        "LocationKey",
        "GradeKey",
        "PersonTypeKey",
        "NationalityKey",
        "GenderKey",
        "DayKey",
        "InformalOrgKey",
        "FormalOrgKey"
      ).map(col),
      count(col("*")).alias("EmployeeCount"),
      sum("is_new_hire").alias("NewHireCount"),
      sum("is_separation").alias("SeparationCount")
    )
  )

  override def transformations: Seq[Rule] = Seq(
    FilterRule(col("is_active") === true),
    FilterRule(lower(col("assignment_status")).isin(whitelist.AssignmentStatuses.map(_.toLowerCase): _*)),
    FilterRule(lower(col("worker_type")).isin(whitelist.WorkerTypes.map(_.toLowerCase): _*)),
    Fact1Table.WithAgeRule,
    Fact1Table.WithNewHireRule,
    Fact1Table.WithSeparationRule
  )

  private def overwriteEmployeeDetail(filteredFullUserInfo: DataFrame,
                                      opts: Map[String, String],
                                      params: Map[String, String]): Unit = {
    val employeeDetail = EmployeeDetailExtractor.extract(filteredFullUserInfo)

    if (!employeeDetail.isEmpty) {
      LOGGER.info("Writing to employee detail table...")
      writer.write(employeeDetail, dimension.EmployeeDetail, opts, params, SaveMode.Overwrite)
      LOGGER.info("...done")
    }
  }
}

object Fact1Table {

  val JoinAgeGroupRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, ageGroupDf: DataFrame): DataFrame =
      fullUserInfoDf.join(ageGroupDf, col("age_group") <=> ageGroupDf("name"))
  }

  val JoinLocationRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, locationDf: DataFrame): DataFrame =
      fullUserInfoDf.join(locationDf, fullUserInfoDf("work_location")(0)("country")("name") <=> locationDf("country"))
  }

  val JoinPersonTypeRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, personTypeDf: DataFrame): DataFrame =
      fullUserInfoDf.join(personTypeDf, fullUserInfoDf("user_type") <=> personTypeDf("name"))
  }

  val JoinGenderRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, genderDf: DataFrame): DataFrame =
      fullUserInfoDf.join(genderDf, fullUserInfoDf("gender") <=> genderDf("name"))
  }

  val JoinGradeRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, gradeDf: DataFrame): DataFrame =
      fullUserInfoDf.join(gradeDf, fullUserInfoDf("grade") <=> gradeDf("name"))
  }

  val JoinNationalityRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, nationalityDf: DataFrame): DataFrame =
      fullUserInfoDf.join(nationalityDf, fullUserInfoDf("nationality") <=> nationalityDf("name"))
  }

  val JoinDayRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, dayDf: DataFrame): DataFrame =
      fullUserInfoDf.join(broadcast(dayDf), Seq("year", "month", "day"))
  }

  val JoinInformalOrgRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, informalOrgDf: DataFrame): DataFrame =
      fullUserInfoDf.join(informalOrgDf, InformalOrgDataExtractor.InformalOrgCols.map {
        case (fsName, dbName) => fullUserInfoDf(fsName) <=> informalOrgDf(dbName)
      }.reduce(_ && _))
  }

  val JoinFormalOrgRule: Rule = new Rule {
    override def apply(fullUserInfoDf: DataFrame, formalOrgDf: DataFrame): DataFrame =
      fullUserInfoDf.join(formalOrgDf, FormalOrgDataExtractor.FormalOrgCols.map {
        case (fsName, dbName) => fullUserInfoDf(fsName) <=> formalOrgDf(dbName)
      }.reduce(_ && _))
  }

  val WithAgeRule: Rule = new Rule {
    override def apply(df: DataFrame): DataFrame =
      df.withColumn("age", datediff(current_date(), col("date_of_birth")) / 365)
        .withColumn("age_group", ageGroupUdf(col("age")))
  }

  val WithNewHireRule: Rule = new Rule {
    override def apply(df: DataFrame): DataFrame =
      df.withColumn("is_new_hire",
                    when(
                      year(col("hire_date")) === year(current_date()) &&
                      dayofyear(col("hire_date")) === dayofyear(current_date()),
                      1
                    ).otherwise(0))
  }

  val WithSeparationRule: Rule = new Rule {
    override def apply(df: DataFrame): DataFrame =
      df.withColumn("is_separation",
        when(
          year(col("end_date")) === year(current_date()) &&
            dayofyear(col("end_date")) === dayofyear(current_date()),
          1
        ).otherwise(0))
  }

  def apply(sparkSession: SparkSession,
            jdbc: String,
            sourcePath: String,
            dataLoadingStrategy: LoadingStrategy): Fact1Table =
    new Fact1Table with JdbcSource {
      override def sourceDataPath: String                     = sourcePath
      override protected def loadingStrategy: LoadingStrategy = dataLoadingStrategy
      override def jdbcUrl: String                            = jdbc
      override def spark: SparkSession                        = sparkSession
    }
}
