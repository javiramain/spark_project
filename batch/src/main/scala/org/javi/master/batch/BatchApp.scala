package org.javi.master.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging


object BatchApp extends Logging {
  def main(args: Array[String]): Unit = {

    try {

      log.info("Creando SparkSession")
      val spark = SparkSession
        .builder()
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.mongodb.output.collection", "articulos")
        .config("spark.mongodb.output.database", "elmercado")
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017")
        //        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
        .appName("ElMercado-BatchApplication")
        .getOrCreate()

      val inputRelativePath = "data/json/input/"
      val inputAbsolutePath = "/" + inputRelativePath
      val sparkMaster = spark.conf.get("spark.master")
      val inputPath = sparkMaster match {
        case "local[*]" => inputRelativePath
        case _ => inputAbsolutePath
      }
      val df = spark.read.option("multiline", "true").json(inputPath)
      df.select("id_articulo", "nombre_articulo", "palabras_clave", "caracteristicas_venta").write
        .format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
    }
    catch {
      case e: Exception =>
        log.error(s"Ocurrio una excepcion de tipo ${e.getClass.getSimpleName} durante la ejecucion:")
        log.error(e.getMessage)
        throw new Exception(e)
    }
  }
}
