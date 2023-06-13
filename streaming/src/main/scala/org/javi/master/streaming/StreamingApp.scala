package org.javi.master.streaming

//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
import com.google.crypto.tink.proto.KmsAeadKeyFormat
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.streaming.StreamingQuery

object StreamingApp  {


  def main(args: Array[String]): Unit = {

//    try {



//      val log = Logger.getRootLogger
//      log.setLevel(Level.ERROR)

//      log.info("starting Spark session")
      val ssc = SparkSession
        .builder()
        .appName("KafkaStreaming")
        .getOrCreate()

      import ssc.implicits._
    val sparkMaster = ssc.conf.get("spark.master")
    val bootstrapServer  = sparkMaster match {
      case "local[*]" =>  "localhost:9095"
      case _ => "workernode1:9092,workernode2:9093,workernode3:9094"
    }
      val KafkaStream = ssc
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("subscribe", "streaming-query")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(key AS STRING) as CLAVE", "CAST(value AS STRING) as BUSQUEDA")
        // cuando pones el writeStream tienes que ponerle el start.

      val user = "user"

    // al poner .read.format("kafka") esta leyendo lo que ya hay en el topic, si metes algo nuevo en esta ejecucion no lo lee.
    // al poner .readStream te lo va leyendo y al poner writeStream.start() lo va escribiendo a medida que lo lee
//    val df = ssc
//      .read
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "streaming-query")
//      .option("startingOffsets", "earliest")
//      .load()

//    println("df")
//    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)].show(false)

    println("KafkaStream")
//    KafkaStream.show(false)
    KafkaStream.writeStream.format("console").outputMode("append")
      .start().awaitTermination()




//      def queryMongo(query: String, idBusqueda: Long, user: String): Unit /*DataFrame*/ = {
        // val df = ssc.read.format("mongo").option("uri", "mongoUri").load()
        // df.filter(col("campo") === query)
//        println(s"El usuario $user quiere comprar $query. Es su $idBusqueda busqueda")
//        log.info(s"Pedido numero $idBusqueda realizado.")
//      }

      //    val userQuery = KafkaStream
      //      .selectExpr(s"CAST(value AS STRING) as query")
      //
      //      .select("query")
      //      .collect().map(_.getString(0))


//      KafkaStream.writeStream.foreachBatch {
//        (batchDF: DataFrame, batchId: Long) =>
//          queryMongo(batchDF.selectExpr(s"CAST(value AS STRING) as query").collect().map(_.getString(0)).mkString, batchId, user)

        //        cuando sea un df lo que obtengo, puedo escribirlo en varios sitios simultaneamente
//      }.start().awaitTermination()
      //    val queryResult = userQuery.map(x => queryMongo(x, user))
      //      .collect()) y flatmap en lugar de map

      //    def processQueryResult(df:DataFrame, batchId: Long):Unit = {
      //      (println(df), println(batchId))
    }

      //    val query: StreamingQuery = queryResult.writeStream
      //      .foreachBatch(queryMongo(userQuery))
      //      .start()
      //        .writeStream
      //      .format("console")
      //      .outputMode("append")

//    catch {
//      case e: Exception =>
//            println(e)
//        log.error(s"Ocurrió una excepción de tipo ${e.getClass.getSimpleName} durante la ejecución:")
//        log.error(e.getMessage)

//    }
//  }

  }

