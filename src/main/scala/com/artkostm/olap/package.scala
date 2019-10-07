package com.artkostm

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

package object olap {
  protected[reporting] val ageGroupUdf: UserDefinedFunction = udf(
    (age: Int) =>
      age match {
        case x if x < 18             => null
        case x if x >= 18 && x <= 24 => "18-24"
        case x if x >= 25 && x <= 34 => "25-34"
        case x if x >= 35 && x <= 44 => "35-44"
        case x if x >= 45 && x <= 54 => "45-54"
        case x if x >= 55            => "55+"
    }
  )

  protected[reporting] object fact {
    val Fact1Table = "reporting.Fact1Table"
    val Fact2Table = "reporting.Fact2Table"
    val Fact3Table = "reporting.Fact3Table"
  }

  protected[reporting] object dimension {
    val PersonType     = "reporting.PersonTypeDim"
    val Gender         = "reporting.GenderDim"
    val Grade          = "reporting.GradeDim"
    val Nationality    = "reporting.NationalityDim"
    val Location       = "reporting.LocationDim"
    val AgeGroup       = "reporting.AgeGroupDim"
    val Day            = "reporting.DayDim"
    val EmployeeDetail = "reporting.EmployeeDetail"
    val InformalOrg    = "reporting.InformalOrgDim"
    val FormalOrg      = "reporting.FormalOrgDim"
  }

  protected[reporting] object whitelist {
    val WorkerTypes: Seq[String]        = Seq("E", "C")
    val AssignmentStatuses: Seq[String] = Seq("A", "B")
  }

  protected[reporting] object blacklist {

    val PersonTypes: Seq[String] = Seq(
      "ex-employee",
      "ex-contingent worker"
    )
  }
}
