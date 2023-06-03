package org.javi.master.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col

object App extends Logging {
  def main(args: Array[String]): Unit = {

    try {

      log.info("Creando SparkSession")
      val spark = SparkSession
        .builder()
        .appName("Simple Application")
        .getOrCreate()

      log.info(s"SparkSession creada correctamente para la aplicacion ${spark.sparkContext.appName}")

      val inputRelativePath = "data/csv/test_data.csv"
      val inputAbsolutePath = "/" + inputRelativePath
      val sparkMaster = spark.conf.get("spark.master")
      val inputPath = sparkMaster match {
        case "local[*]" => inputRelativePath
        case _ => inputAbsolutePath
      }
      val df = spark.read.option("header", "true").csv(inputPath)


      println(df.count())
      println(spark.conf.get("spark.master"))

//      Thread.sleep(120000)
      df.show(10, truncate = false)

      val left = df.select(col("PPPPP"), col("matnr"), col("lgort"))
      val right = df.select("werks", "matnr", "zcat")

      val finalDf = left.join(right, Seq("werks", "matnr"))
      val outputRelativePath = "data/parquet/"
      val outputAbsolutePath = "/" + outputRelativePath
      val outputPath = sparkMaster match {
        case "local[*]" => outputRelativePath
        case _ => outputAbsolutePath
      }
      finalDf.write.mode("append").parquet(outputPath)
    }
    catch {
      case e: Exception =>
        log.error(s"Ocurrió una excepción de tipo ${e.getClass.getSimpleName} durante la ejecución:")
        log.error(e.getMessage)
    }
  }
}
