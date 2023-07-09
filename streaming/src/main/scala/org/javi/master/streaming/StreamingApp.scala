package org.javi.master.streaming


import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, desc, lit, size, when}
import org.apache.spark.sql.types.{StringType}
import org.apache.spark.sql.SparkSession


object StreamingApp extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .set("spark.mongodb.read.connection.uri", "mongodb://masternode:27017/elmercado.articulos")
      .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
      .set("spark.sql.streaming.checkpointLocation", "/tmp")
      .set("spark.driver.memory", "1g")
      .set("spark.executor.memory", "1g")

    log.info("starting Spark session")
    val ssc = SparkSession
      .builder()
      .config(sparkConf)
      .appName("ElMercado-StreamingApplication")
      .getOrCreate()

    import ssc.implicits._
    val sparkMaster = ssc.conf.get("spark.master")
    val bootstrapServer = sparkMaster match {
      case "local[*]" => "localhost:9095"
      case _ => "workernode1:9092,workernode2:9093,workernode3:9094"
    }

    val mongoData = ssc.read
      .format("mongodb")
      .load()

    println("mongo read correctly")

    val flattenedDf = mongoData
      .select("nombre_articulo", "palabras_clave", "caracteristicas_venta", "caracteristicas_venta.*")
      .withColumn("valores", concat(lit("Articulo: "), col("nombre_articulo"), lit("\nCaracteristicas: \n")))

    flattenedDf.show(false)
    val columns = flattenedDf.drop("nombre_articulo", "palabras_clave", "caracteristicas_venta", "valores").schema.fieldNames
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

    // Leer los mensajes desde el topic de Kafka
    val kafkaDF = ssc.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", "streaming-query")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as BUSQUEDA")

    kafkaDF
      .writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
        if (batchDF.count() == 1) {
          val busqueda = batchDF.select("BUSQUEDA").collect()(0).mkString.replace("\"", "")
            .split(" ")
          println("la busqueda es " + busqueda.mkString)

          val output = flattenedMongo
            .withColumn("busqueda", lit(busqueda))
            .withColumn("inters_size", size(array_intersect(col("busqueda"), col("palabras_clave"))))
            .filter(col("inters_size") > 0)
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
