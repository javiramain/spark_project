package org.javi.master.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Simple Application")
      .getOrCreate()

    val inputRelativePath = "data/csv/test_data.csv"
    val inputAbsolutePath = "/"+inputRelativePath
    val sparkMaster = spark.conf.get("spark.master")
    val inputPath = sparkMaster match {
      case "local[*]" => inputRelativePath
      case _ => inputAbsolutePath
    }
    val df = spark.read.option("header", "true").csv(inputPath)


    println(df.count())
    println(spark.conf.get("spark.master"))

    Thread.sleep(120000)
    df.show(10, false)

    val left = df.select(col("werks"), col("matnr"), col("lgort"))
    val right = df.select("werks", "matnr", "zcat")

    val finalDf = left.join(right, Seq("werks", "matnr"))
//    finalDf.show(20, false)
    val outputRelativePath = "data/parquet/"
    val outputAbsolutePath = "/"+outputRelativePath
    val outputPath = sparkMaster match {
      case "local[*]" => outputRelativePath
      case _ => outputAbsolutePath
    }
    finalDf.write.mode("append").parquet(outputPath)

  }
}
