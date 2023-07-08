package org.javi.master.streaming


import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, desc, explode, lit, map_concat, map_from_entries, size, split, typedLit, when}
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.slf4j.LoggerFactory

import java.util.Properties
//import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}


object StreamingApp extends Logging {


  def main(args: Array[String]): Unit = {

      val sparkConf = new SparkConf()
        .set("spark.mongodb.read.connection.uri", "mongodb://masternode:27017/elmercado.articulos")
        .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
        .set("spark.sql.streaming.checkpointLocation", "/tmp")

      log.info("starting Spark session")
      val ssc = SparkSession
        .builder()
        .config(sparkConf)
        .appName("ElMercado-StreamingApplication")
        .getOrCreate()

      import ssc.implicits._
      val sparkMaster = ssc.conf.get("spark.master")
      val (bootstrapServer, mongoURI) = sparkMaster match {
        case "local[*]" => ("localhost:9095", "mongodb://localhost:27017/elmercado.articulos")
        case _ => ("workernode1:9092,workernode2:9093,workernode3:9094", "mongodb://masternode:27018/elmercado.articulos")
      }


      val mongoDataBase = ssc.read
        .format("mongodb")
//        .option("uri", mongoURI)
        .load()
    mongoDataBase.show(2)
//    println("mongodatabase:")
    println("mongo read correctly")
    val flattenedDf = mongoDataBase
      .select( "nombre_articulo","palabras_clave", "caracteristicas_venta", "caracteristicas_venta.*")
      .withColumn("valores",concat(lit("Articulo: "),col("nombre_articulo"),lit("\nCaracteristicas: \n")))
    val columns = flattenedDf.drop( "nombre_articulo", "palabras_clave", "caracteristicas_venta", "valores").schema.fieldNames

//    flattenedDf.show(false)
    val flattenedMongo = columns.foldLeft(flattenedDf) { (tempDf, colName) =>
        tempDf
          .withColumn("valores", when(
            col(colName).isNotNull and !col("valores").endsWith(": \n"), concat_ws(s", ", col("valores"), concat_ws(": ", lit(colName), col(colName)))
          ).
            when(
              col(colName).isNotNull, concat(col("valores"), concat_ws(": ", lit(colName), col(colName))))
                .otherwise(col("valores"))).cache

    }
      .select("nombre_articulo", "palabras_clave", "valores", "caracteristicas_venta")
// Meter un toLowerCase en la busqueda y en las palabras clave

    println("caracteristicas venta mapeadas")
    flattenedMongo.show(false)
//    flattenedMongo.show(false)


    // Leer los mensajes desde el topic de Kafka
      val kafkaDF = ssc.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",bootstrapServer)
        .option("subscribe", "streaming-query")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as BUSQUEDA")

      kafkaDF
        .writeStream
        .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
          if (batchDF.count() == 1) {
            val busqueda = batchDF.select("BUSQUEDA").collect()(0).mkString.replace("\"", "")
              .toLowerCase.split(" ")
            val output = flattenedMongo
              .withColumn("busqueda", lit(busqueda))
              .withColumn("inters_size", size(array_intersect(col("busqueda"), col("palabras_clave"))))
              .filter(col("inters_size") > 0)
              //            .filter(col("palabras_clave").contains(a))
              .orderBy(desc("inters_size"))
              .select(
                col("valores").cast(StringType).as("value"))


            val dummyData = Seq("No se ha encontrado ningun artículo con esas palabras clave")
            val noSuchArticleMessage = ssc.sparkContext.parallelize(dummyData).toDF("value")

            output.count() match {
              case 0 =>
                println("No se ha encontrado ningun articulo")
                noSuchArticleMessage
                  .write
                  .format("kafka")
                  .option("kafka.bootstrap.servers", bootstrapServer)
                  .option("topic", "output")
                  .save
              //              ()
              case _ =>
                println("escribiendo resultados")
                output
                  .write
                  .format("kafka")
                  .option("kafka.bootstrap.servers", bootstrapServer)
                  .option("topic", "output")
                  .save
            }
            println("saved results")
          }
        }
          .start().awaitTermination()
  }
}
