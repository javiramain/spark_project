package org.javi.master.streaming

//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.log4j.LoggerFac
//import org.apache.spark.SparkConf
import org.apache.log4j.PropertyConfigurator
import com.google.crypto.tink.proto.KmsAeadKeyFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory
//import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}


object StreamingApp {


  def main(args: Array[String]): Unit = {
    val log4jConfPath = "src/main/resources/log4j2.properties"
    PropertyConfigurator.configure(log4jConfPath)
    val log = LoggerFactory.getLogger(getClass)
//    conf.set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector:10.0.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
    try {

      val sparkConf = new SparkConf()
        .set("spark.driver.extraJavaOptions", s"-Dlog4j.configuration=file:$log4jConfPath")
        .set("spark.executor.extraJavaOptions", s"-Dlog4j.configuration=file:$log4jConfPath")
        .set("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/localDb.Usuarios")

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
      val KafkaStream = ssc
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("subscribe", "streaming-query")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as BUSQUEDA")
      // cuando pones el writeStream tienes que ponerle el start.

      val a = KafkaStream.select("BUSQUEDA").map(r => r.getString(0)).collect.flatten.mkString
      println(a)
      val mongoDataBase = ssc.read
//        .format("com.mongodb.spark.sql.DefaultSource")
        .format("mongodb")
        .load()


      println("consulta")
      val mongoInfo = mongoDataBase.select(col("nombre"))
        .filter(col("apellido").contains(a))
//        .show(false)
      val user = "user"

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


    }
    catch {
      case e: Exception =>
        println(e)
        log.error(s"Ocurrió una excepción de tipo ${e.getClass.getSimpleName} durante la ejecución:")
        log.error(e.getMessage)
    }
  }
}

