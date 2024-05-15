package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Part3 {

  val spark = SparkSession.builder().appName("Part1").master("local[*]").getOrCreate()

  val schema = StructType(Array(
    StructField("App", StringType),
    StructField("Categories", StringType),
    StructField("Rating", DoubleType),
    StructField("Reviews", LongType),
    StructField("Size", StringType),
    StructField("Installs", StringType),
    StructField("Type", StringType),
    StructField("Price", DoubleType),
    StructField("Content_Rating", StringType),
    StructField("Genres", StringType),
    StructField("Last_Updated", StringType),
    StructField("Current_Version", StringType),
    StructField("Minimum_Android_Version", StringType)
  ))


  val path = "src/main/resources/googleplaystore.csv"
  val df_3 = spark.read
    .format("csv")
    .option("delimiter", ",")
    .schema(schema)
    .load(path)

  df_3.createOrReplaceTempView("googleplaystore") //SQL temporary view


  // final data frame
  val sqlDF3 = spark.sql("SELECT App ," +
    "COLLECT_LIST(DISTINCT Categories) AS Categories , " +
    "FIRST(Rating) as Rating," +
    "MAX(Reviews) , " +
    "CASE " +
    "    WHEN MAX(Size) LIKE '%M' THEN CAST(REGEXP_REPLACE(MAX(Size), '[^0-9.]', '') AS DOUBLE) " +
    "    WHEN MAX(Size) LIKE '%k' THEN CAST(REGEXP_REPLACE(MAX(Size), '[^0-9.]', '') AS DOUBLE) / 1024 " +
    "    ELSE NULL " +
    "END as Size," +
    "FIRST(Installs) ," +
    "FIRST(Type) ," +
    "CONCAT('€', CAST(REGEXP_REPLACE(FIRST(Price), '[^0-9.]', '') AS DOUBLE) * 0.9) as Price," +
    "FIRST(`Content_Rating`) AS ContentRating ," +
    "SPLIT(FIRST(Genres), ';') AS Genres ," +
    "TO_DATE(FIRST(`Last_Updated`), 'MMMM d, yyyy') AS LastUpdate," +
    "FIRST(`Current_Version`) AS CurrentVer," +
    "FIRST(`Minimum_Android_Version`) AS AndroidVer" +

    " FROM googleplaystore " +
    "GROUP BY App")







}
