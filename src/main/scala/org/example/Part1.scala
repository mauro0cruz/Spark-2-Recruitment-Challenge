package org.example
import org.apache.spark.sql.SparkSession

object Part1 {

  val spark = SparkSession.builder().appName("Part1").master("local[*]").getOrCreate()
  val path = "src/main/resources/googleplaystore_user_reviews.csv"
  val df1 = spark.read.csv(path) //data frame with all data

  df1.createOrReplaceTempView("googleplaystore_user_reviews") //SQL temporary view


  // final data frame
  val sqlDF1 = spark.sql("select _c0 as App,IF(isnan(AVG(_c3)), 0.0, AVG(_c3)) AS Average_Sentiment_Polarity  " +
    "from googleplaystore_user_reviews " +
    "group by _c0")


}



