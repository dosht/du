ThisBuild / organization := "com.example"
ThisBuild / scalaVersion := "2.13.5"

lazy val root = (project in file(".")).settings(
  name := "du",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.3.1",
    "org.scalactic" %% "scalactic" % "3.2.15",
    "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
    "org.scalatest" %% "scalatest" % "3.2.15" % Test,
    "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
    "org.scalameta" %% "munit" % "0.7.29" % Test,
    "io.findify" %% "s3mock" % "0.2.6" % "test"
  )
)
