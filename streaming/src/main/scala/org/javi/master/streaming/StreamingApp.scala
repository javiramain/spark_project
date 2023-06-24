package org.javi.master.streaming

//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.log4j.LoggerFac
//import org.apache.spark.SparkConf
import org.apache.log4j.PropertyConfigurator
import com.google.crypto.tink.proto.KmsAeadKeyFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StringType, StructType}
import org.slf4j.LoggerFactory

import java.util.Properties
//import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}


object StreamingApp extends Logging{


  def main(args: Array[String]): Unit = {
//    val log4jConfPath = "src/main/resources/log4j2.properties"
//    PropertyConfigurator.configure(log4jConfPath)
    val log = LoggerFactory.getLogger(getClass)
//    conf.set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector:10.0.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
//    try {

      val sparkConf = new SparkConf()
//        .set("spark.driver.extraJavaOptions", s"-Dlog4j.configuration=file:$log4jConfPath")
//        .set("spark.executor.extraJavaOptions", s"-Dlog4j.configuration=file:$log4jConfPath")
        .set("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/localDb.Usuarios")
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
      val bootstrapServer = sparkMaster match {
        case "local[*]" => "localhost:9095"
        case _ => "workernode1:9092,workernode2:9093,workernode3:9094"
      }
      /*

      mongoInfo
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("topic", "mongoOutput")
        .save()


//      println("KafkaStream")
      // Si la query es de una fuente en streaming, hay que ejecutarla con writeStream.start()
//      KafkaStream.writeStream
      //      .format("console").outputMode("append")
//        .start().awaitTermination()

*/
      val mongoDataBase = ssc.read
        .format("mongodb")
        .load()

      // Leer los mensajes desde el topic de Kafka
      val kafkaDF = ssc.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",bootstrapServer)
        .option("subscribe", "streaming-query")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as BUSQUEDA")

//    kafkaDF.toJSON.writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers",bootstrapServer)
//      .option("topic", "output")
//      .start().awaitTermination()

//

//
//      val a = kafkaDF.select("BUSQUEDA").writeStream.outputMode("memory").start()
//
//      val mongoInfo = mongoDataBase
//        .select(col("nombre"))
//        .as[String]


//    val output = mongoDataBase
    //            .as[String]
//    output.show(false)
//    output
//      .write
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9095")
//      .option("topic", "output")
//      .save()

      kafkaDF
        .writeStream
        .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
//          val filtro = batchDF.select("BUSQUEDA")
          batchDF.show(false)
          //meterle un try por si cuando se ejecute no se ha recibido ningun mensaje aun
          val a = batchDF.select("BUSQUEDA").collect()(0).mkString
          val output = mongoDataBase.filter(col("apellido")===a).select("nombre")
            .withColumn("value", col("nombre"))
//            .as[String]
          output.show(false)
          output
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9095")
            .option("topic", "output")
//            .format("parquet").mode("append")
//            .save("/home/javi/Documentos/TFM/spark_project/streaming/salida/")
            .save

          println("saved results")
          ()
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
