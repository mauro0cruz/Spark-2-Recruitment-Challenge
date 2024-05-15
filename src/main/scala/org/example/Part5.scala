package org.example

import org.apache.spark.sql.{SparkSession, functions}
import org.example.Part4.df3_1
import org.apache.spark.sql.functions.{avg, col, count, explode}


object Part5 {

  val spark = SparkSession.builder().appName("Part1").master("local[*]").getOrCreate()


  val explodedDf3 = df3_1.withColumn("Genres", explode(col("Genres")))

  // Registre o DataFrame resultante como uma visualização temporária
  explodedDf3.createOrReplaceTempView("exploded_df3")

  // Calcule as estatísticas desejadas para cada gênero
  val df_4 = spark.sql("""
    SELECT DISTINCT Genres,
           COUNT(DISTINCT App) ,
           AVG(Rating), FIRST(Average_Sentiment_Polarity)
    FROM exploded_df3
    GROUP BY Genres
""")
}


