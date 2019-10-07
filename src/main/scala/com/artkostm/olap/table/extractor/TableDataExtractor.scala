package com.artkostm.olap.table.extractor

import com.artkostm.olap.blacklist
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import com.artkostm.olap.table.extractor.TableDataExtractor._

trait TableDataExtractor {
  def extract(source: DataFrame): DataFrame
}

object TableDataExtractor {
  def renameCols(mappings: Map[String, String], dataFrame: DataFrame): DataFrame =
    mappings.foldLeft(dataFrame) {
      case (df, (oldName, newName)) => df.withColumnRenamed(oldName, newName)
    }
}

object LocationDataExtractor extends TableDataExtractor {
  override def extract(fullUserInfoDf: DataFrame): DataFrame =
    fullUserInfoDf
      .withColumn("work_location", col("work_location")(0))
      .withColumn("country", col("work_location")("country")("name"))
      .select("country")
      .distinct()
}

object NationalityDataExtractor extends TableDataExtractor {
  override def extract(fullUserInfoDf: DataFrame): DataFrame =
    fullUserInfoDf
      .select(col("nationality").alias("name"))
      .distinct()
}

object GenderDataExtractor extends TableDataExtractor {
  override def extract(fullUserInfoDf: DataFrame): DataFrame =
    fullUserInfoDf
      .select(col("gender").alias("name"))
      .distinct()
}

object GradeDataExtractor extends TableDataExtractor {
  override def extract(fullUserInfoDf: DataFrame): DataFrame =
    fullUserInfoDf
      .select(col("grade").alias("name"))
      .distinct()
}

object PersonTypeDataExtractor extends TableDataExtractor {

  override def extract(fullUserInfoDf: DataFrame): DataFrame =
    fullUserInfoDf
      .select(col("user_type").alias("name"))
      .filter(not(lower(col("name")).isin(blacklist.PersonTypes: _*)))
      .distinct()
}

object EmployeeDetailExtractor extends TableDataExtractor {

  final case class Manager(email: String)

  val EmployeeDetailColumnMappings: Map[String, (String, String)] = Map(
    "employee_number"               -> ("id.employee_number", "EmployeeNumber"),
    "full_name"                     -> ("employee_name.full_name", "EmployeeName"),
    "user_type"                     -> ("user_type", "PersonType"),
    "org_function"                  -> ("org_function", "Function"),
    "org_department"                -> ("org_department", "Department"),
    "org_discipline"                -> ("org_discipline", "Discipline"),
    "org_business_unit"             -> ("org_business_unit", "BusinessUnit"),
    "org_service_line"              -> ("org_service_line", "ServiceLine"),
    "work_location[0].code"         -> ("work_location[0].code", "Location"),
    "work_location[0].country.name" -> ("work_location[0].country.name", "Country"),
    "email"                         -> ("email", "Email"),
    "worker_type"                   -> ("worker_type", "EmploymentType"),
    "assignment_status"             -> ("assignment_status", "AssignmentStatus"),
    "business_group"                -> ("business_group", "BusinessGroup"),
    "operating_unit"                -> ("operating_unit", "OperatingUnit"),
    "manager_name"                  -> ("managers.name.full_name as manager_name", "ManagerName"),
    "manager_number"                -> ("managers.id.employee_number as manager_number", "ManagerNumber"),
    "managers_hierarchy"            -> ("managers_hierarchy", "managers_hierarchy")
    // "???" -> ("???", "ManagerActive")
  )

  val getDirectManagerUdf: UserDefinedFunction = udf((xs: Seq[Manager]) => if (xs != null) xs.lastOption else None)

  val lastDaySpec: WindowSpec = Window.orderBy(col("ExactDate").desc)

  override def extract(fullUserInfo: DataFrame): DataFrame =
    EmployeeDetailColumnMappings.foldLeft {
      fullUserInfo
        .withColumn("_last_day", first("ExactDate").over(lastDaySpec)) // retrieve data only for the last day
        .where(col("_last_day") === col("ExactDate"))
        .drop("_last_day")
        .selectExpr(EmployeeDetailColumnMappings.map(_._2._1).toSeq: _*)
    } {
      case (df, (oldName, (_, newName))) => df.withColumnRenamed(oldName, newName)
    }.withColumn("ManagerEmail", getDirectManagerUdf(col("managers_hierarchy"))("email"))
      .drop("managers_hierarchy")

}

object InformalOrgDataExtractor extends TableDataExtractor {

  val InformalOrgCols: Map[String, String] = Map(
    "org_business_unit" -> "BusinessUnit",
    "org_service_line"  -> "ServiceLine",
    "org_function"      -> "Function",
    "org_department"    -> "Department"
  )

  override def extract(fullUserInfo: DataFrame): DataFrame =
    renameCols(
      InformalOrgCols,
      fullUserInfo.select(InformalOrgCols.keys.toSeq.map(col): _*).distinct()
    )
}

object FormalOrgDataExtractor extends TableDataExtractor {
  val FormalOrgCols: Map[String, String] = Map(
    "business_group" -> "BusinessGroup",
    "operating_unit" -> "OperatingUnit"
  )

  override def extract(fullUserInfo: DataFrame): DataFrame =
    renameCols(
      FormalOrgCols,
      fullUserInfo.select(FormalOrgCols.keys.toSeq.map(col): _*).distinct()
    )
}
