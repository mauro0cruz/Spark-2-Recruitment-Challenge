package org.example
import org.apache.spark.sql.SparkSession


object Part2 {

  val spark = SparkSession.builder().appName("Part2").master("local[*]").getOrCreate()
  val path = "src/main/resources/googleplaystore.csv"

  val df2 = spark.read.option("delimiter", ",").csv(path)

  df2.createOrReplaceTempView("googleplaystore") //SQL temporary view


  // final data frame => hide null lines
  val sqlDF2 = spark.sql("select * from googleplaystore where NOT isnan(_c2) AND _c2 >= 4.0 ORDER BY _c2 DESC")


}
