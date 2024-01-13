ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.13.12"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.3.3"

lazy val root = (project in file("."))
  .settings(
    name := "Zadanie",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )
