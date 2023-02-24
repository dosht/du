package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.SparkContext._
import scala.util.Try

object Main extends App {
  def splitLines(rdd: RDD[String]): RDD[(String, String)] = rdd.flatMap { line =>
    val csv = line.split(",")
    if (csv.length == 2) {
      Seq(csv)
    } else {
      val tsv = line.split("\t")
      if (tsv.length == 2) {
        Seq(tsv)
      } else {
        Seq.empty
      }
    }
  }.map { case Array(key, value) => key -> value }


  def filterNumbers(rdd: RDD[(String, String)]): RDD[(String, String)] =
    rdd.filter { case (key, value) => Try(key.toInt).isSuccess && Try(value.toInt).isSuccess }

  def filterOddNumbers(rdd: RDD[(String, String)]): RDD[(String, String)] =
    rdd.groupByKey().flatMapValues(_.groupBy(identity).map(x => x._1 -> x._2.size).filter(_._2 % 2 == 1).keys)

  if (args.length < 2) {
    println("Error - missing arguments. Expected input-s3 and output-s3")
  }
  val Array(inputPath, outputPath) = args
  val spark: SparkSession = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
  import spark.sqlContext.implicits._

  // Alternative solution to read both csv and tsv
  val csvs = spark.read.option("delimiter", ",").option("header", "false").csv(s"s3a://$inputPath/*.csv")
  val tsvs = spark.read.option("delimiter", "\t").option("header", "false").csv(s"s3a://$inputPath/*.tsv")
  val unionDD: RDD[(String, String)] = csvs.union(tsvs).map(row => row.get(0).toString -> row(1).toString).rdd


  val inputRDD = spark.sparkContext.textFile(s"s3a://$inputPath/*")
  filterNumbers(filterNumbers(splitLines(inputRDD))).toDF.write
    .option("header", "true")
    .option("delimiter", "\t")
    .csv(s"s3a://$outputPath")
}