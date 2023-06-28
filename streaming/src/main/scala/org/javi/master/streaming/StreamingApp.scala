package org.javi.master.streaming

//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.log4j.LoggerFac
//import org.apache.spark.SparkConf
import org.apache.log4j.PropertyConfigurator
import com.google.crypto.tink.proto.KmsAeadKeyFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, desc, explode, lit, map_concat, map_from_entries, size, split, typedLit, when}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.slf4j.LoggerFactory

import java.util.Properties
//import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}


object StreamingApp extends Logging {


  def main(args: Array[String]): Unit = {
//    val log4jConfPath = "src/main/resources/log4j2.properties"
//    PropertyConfigurator.configure(log4jConfPath)
//    val log = LoggerFactory.getLogger(getClass)
//    conf.set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector:10.0.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
//    try {


      val sparkConf = new SparkConf()
//        .set("spark.driver.extraJavaOptions", s"-Dlog4j.configuration=file:$log4jConfPath")
//        .set("spark.executor.extraJavaOptions", s"-Dlog4j.configuration=file:$log4jConfPath")
        .set("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/elmercado.articulos")
//        .set("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/streamingdb.usuarios") //conectar solamente con el master (PRIMARY)

        .set("spark.sql.streaming.checkpointLocation", "/tmp")

      log.info("starting Spark session")
      val ssc = SparkSession
        .builder()
        .config(sparkConf)
        .appName("KafkaStreaming")
//        .config(sparkConf)
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
//    println("mongodatabase:")

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

//    flattenedMongo.show(false)


    // Leer los mensajes desde el topic de Kafka
      val kafkaDF = ssc.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",bootstrapServer)
        .option("subscribe", "streaming-query")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as BUSQUEDA")

      kafkaDF
        .writeStream
        .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>

          val busqueda = batchDF.select("BUSQUEDA").collect()(0).mkString.split(" ")
//            .filter( _ != " ")
          val output = flattenedMongo
            .withColumn("busqueda", lit(busqueda))
            .withColumn("inters_size", size(array_intersect(col("busqueda"), col("palabras_clave"))))
            .filter(col("inters_size") > 0)
//            .filter(col("palabras_clave").contains(a))
            .orderBy(desc("inters_size"))
            .select(
//              col("nombre_articulo").as("key"),
              col("valores").cast(StringType).as("value"))


          val dummyData = Seq("No se ha encontrado ningun artículo con esas palabras clave")
          val noSuchArticleMessage = ssc.sparkContext.parallelize(dummyData).toDF( "value")

          output.count() match {
            case 0 =>
              noSuchArticleMessage
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9095")
                .option("topic", "output")

                .save
//              ()
            case _ =>
              output
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9095")
                .option("topic", "output")

                .save
//              ()
          }


          println("saved results")
//          ()
        }
          .start().awaitTermination()

//      kafkaDF.writeStream.start()

      //
//
//
//      val query = mongoInfo
//        .writeStream
//        .format("kafka")
//        .option("kafka.bootstrap.servers", "localhost:9095")
//        .option("topic", "output")
//        .start()
////        .foreachBatch { (batchDF, batchId) =>
////          batchDF.persist() // Persistir el DataFrame para optimizar el rendimiento en caso de múltiples operaciones
////          val resultados = batchDF.collect() // Obtener los resultados del batchDF
////          resultados.foreach(resultado => writer.process(resultado)) // Enviar cada resultado a Kafka
////          batchDF.unpersist() // Liberar la persistencia del DataFrame
////        }
//      // Iniciar la consulta
//      query.awaitTermination()
//

//    }
//    catch {
//      case e: Exception =>
//        println(e)
//        log.error(s"Ocurrió una excepción de tipo ${e.getClass.getSimpleName} durante la ejecución:")
//        log.error(e.getMessage)
//    }
  }
}
