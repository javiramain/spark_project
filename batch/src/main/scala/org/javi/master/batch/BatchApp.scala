package org.javi.master.batch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import com.mongodb.spark._

object BatchApp extends Logging {
  def main(args: Array[String]): Unit = {

    try {

      val logger = Logger.getLogger("BatchApp")
      logger.setLevel(Level.INFO)
      logger.info("Creando Spark Session de la aplicación")
      log.info("Creando SparkSession")
      val spark = SparkSession
        .builder()
        .config("spark.mongodb.output.collection", "articulos")
        .config("spark.mongodb.output.database", "elmercado")
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017")
        //        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
        .appName("ElMercado-BatchApplication")
        .getOrCreate()

      logger.info(s"SparkSession creada correctamente para la aplicacion ${spark.sparkContext.appName}")

      val inputRelativePath = "data/json/input/"
      val inputAbsolutePath = "/" + inputRelativePath
      val sparkMaster = spark.conf.get("spark.master")
      val inputPath = sparkMaster match {
        case "local[*]" => inputRelativePath
        case _ => inputAbsolutePath
      }
      val df = spark.read.option("multiline","true").json(inputPath)
      df.select("id_articulo", "nombre_articulo", "palabras_clave", "caracteristicas_venta").write
        .format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
    }
    catch {
      case e: Exception =>
        log.error(s"Ocurrió una excepción de tipo ${e.getClass.getSimpleName} durante la ejecución:")
        log.error(e.getMessage)
        throw new Exception(e)
    }
  }
}
