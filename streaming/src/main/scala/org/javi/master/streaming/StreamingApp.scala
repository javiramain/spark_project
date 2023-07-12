package org.javi.master.streaming


import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{array_intersect, col, concat, concat_ws, max, lit, size, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType


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

    val caracteristicas_venta = mongoData.select("caracteristicas_venta").schema.fields.head.dataType.asInstanceOf[StructType].fields

    val keyValueColumns = caracteristicas_venta.map { field =>
      val colName = field.name
      val colValue = col(s"caracteristicas_venta.$colName")
      when(colValue.isNotNull, concat_ws(", ", concat_ws(":", lit(colName),colValue)))
    }

    val articlesDataFrame = mongoData
      .withColumn("clave_valor", concat_ws(", ", keyValueColumns: _*))
      .withColumn("valores", concat(lit("Articulo: "), col("nombre_articulo"), lit("\nCaracteristicas: \n"), col("clave_valor")))
      .select("nombre_articulo", "palabras_clave", "valores")

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
            .toLowerCase.split(" ")

          val output = articlesDataFrame
            .withColumn("busqueda", lit(busqueda))
            .withColumn("inters_size", size(array_intersect(col("busqueda"), col("palabras_clave"))))
            .filter(col("inters_size") > 0)
            .withColumn("max_value", max("inters_size").over())
            .filter(col("max_value") === col("inters_size"))
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
              println(s"Se han encontrado ${output.count()} articulos que podrian interesarte:")
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
