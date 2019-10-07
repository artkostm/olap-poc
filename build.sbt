lazy val ziogram = (project in file(".")).settings(
  name := "olap-poc",
  version := "1.0",
  scalaVersion := "2.12.10",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.4" % Provided,
    "org.apache.spark" %% "spark-sql" % "2.4.4" % Provided
  )
)
