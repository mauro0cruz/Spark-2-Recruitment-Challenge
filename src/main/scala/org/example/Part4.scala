package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.example.Part1.sqlDF1
import org.example.Part3.sqlDF3

object Part4 {

  val spark = SparkSession.builder().appName("Part1").master("local[*]").getOrCreate()

  val df1Selected = sqlDF1.select(col("App"), col("Average_Sentiment_Polarity"))
  val df3_1 = sqlDF3.join(df1Selected, sqlDF3("App") === df1Selected("App"), "inner").drop(df1Selected("App"))


//FALTA METER UM ZIP








}
