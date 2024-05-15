package org.example

import org.apache.spark.sql.SparkSession
import org.example.Part2.sqlDF2
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.col
import org.example.Part1.sqlDF1
import org.example.Part3.sqlDF3
import org.example.Part4.df3_1
import org.example.Part5.df_4


object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Main").master("local[*]").getOrCreate()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)



    //Part1
    //sqlDF1.show()
    sqlDF1
      .repartition(1)
      .write.format("csv")
      .option("delimiter", ",")
      .save("src/main/resources/part1")

    val files1 = fs.globStatus(new Path("src/main/resources/part1/part-*"))
    fs.rename( files1(0).getPath(), new Path("src/main/resources/part1.csv"))
    fs.delete(new Path("src/main/resources/.part1.csv.crc"), true)
    fs.delete(new Path("src/main/resources/part1"), true)




    //Part2
    sqlDF2.coalesce(1).write.option("delimiter", "§").csv("src/main/resources/part2")

    val files2 = fs.globStatus(new Path("src/main/resources/part2/part-*"))
    fs.rename( files2(0).getPath(), new Path("src/main/resources/best_apps.csv"))
    fs.delete(new Path("src/main/resources/.best_apps.csv.crc"), true)
    fs.delete(new Path("src/main/resources/part2"), true)




    //Part3
    //sqlDF3.show()
    sqlDF3.withColumn("Categories", col("Categories").cast("string"))
      .withColumn("Genres", col("Genres").cast("string"))
      .write
      .option("delimiter", ",")
      .csv(path = "src/main/resources/part3")

    val files3 = fs.globStatus(new Path("src/main/resources/part3/part-*"))
    fs.rename( files3(0).getPath(), new Path("src/main/resources/part3.csv"))
    fs.delete(new Path("src/main/resources/.part3.csv.crc"), true)
    fs.delete(new Path("src/main/resources/part3"), true)



    //Part4
    //df3_1.show()
    df3_1.withColumn("Categories", col("Categories").cast("string"))
      .withColumn("Genres", col("Genres").cast("string"))
      .write
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("src/main/resources/part4")

    val files4 = fs.globStatus(new Path("src/main/resources/part4/part-*"))
    fs.rename( files4(0).getPath(), new Path("src/main/resources/googleplaystore_cleaned.csv.gz"))
    fs.delete(new Path("src/main/resources/.googleplaystore_cleaned.csv.gz.crc"), true)
    fs.delete(new Path("src/main/resources/part4"), true)



    //Part5
    //df_4.show()
    df_4
      .withColumn("Genres", col("Genres").cast("string"))
      .write
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("src/main/resources/part5")

    val files5 = fs.globStatus(new Path("src/main/resources/part5/part-*"))
    fs.rename( files5(0).getPath(), new Path("src/main/resources/googleplaystore_metrics.csv.gz"))
    fs.delete(new Path("src/main/resources/.googleplaystore_metrics.csv.gz.crc"), true)
    fs.delete(new Path("src/main/resources/part5"), true)




    spark.stop()

  }
}
