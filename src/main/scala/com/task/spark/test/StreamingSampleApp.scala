package com.task.spark.test

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

import scala.language.implicitConversions
import com.task.spark.test.model.KafkaRow

object SparkStreamingKube extends App {

  override def main(args: Array[String]): Unit = {

    val mysql_host_name = "localhost"
    val mysql_port_no = "3306"
    val mysql_user_name = "root"
    val mysql_password = "root"
    val mysql_database_name = "test_db"
    val mysql_driver_class = "com.mysql.jdbc.Driver"
    val mysql_table_name = "tweet_texts_new5"
    val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

    val TOPIC = "test-new-topic"

    val spark = SparkSession.builder().appName("twitter").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val ds : Dataset[KafkaRow] = spark.readStream.format("kafka")

      .option("subscribe", TOPIC)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest").option("failOnDataLoss", "false")
      .load().as[KafkaRow]

    ds.selectExpr("CAST(value AS STRING)")

    import spark.implicits._

    val jsonsSchema =
      StructType(Array(
      StructField("user", StructType(Array(
        StructField("id", LongType)))),
      StructField("text", StringType),
      StructField("timestamp_ms", StringType)))

    val df = ds.select(from_json($"value".cast("string"), jsonsSchema).as("tweet")).filter($"tweet.text".isNotNull)

    val df2 = df.select($"tweet.text".as("tweet_text"), to_timestamp(from_unixtime($"tweet.timestamp_ms" / 1000)).as("tweet_timestamp"),
      $"tweet.user.id".as("tweet_id")).filter($"tweet_text".contains("music"))
      .dropDuplicates("tweet_text")
      .groupBy($"tweet_id", window($"tweet_timestamp", "10 seconds")).count()

      .select(
        $"tweet_id",
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"count"
      )

    df2.printSchema()

    val mysql_properties = new java.util.Properties
    mysql_properties.setProperty("driver", mysql_driver_class)
    mysql_properties.setProperty("user", mysql_user_name)
    mysql_properties.setProperty("password", mysql_password)

    println("mysql_jdbc_url: " + mysql_jdbc_url)

    val writer1 = df2.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update")
      .option("checkpointLocation", "path to the file in your local")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchDF_1 = batchDF.
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database
        write.mode("append").jdbc(mysql_jdbc_url, mysql_table_name, mysql_properties)
      }.start()

    val writer = df2.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option("queryName", "spark-kafka-kube")
      .start()

    writer.awaitTermination()

  }


}

