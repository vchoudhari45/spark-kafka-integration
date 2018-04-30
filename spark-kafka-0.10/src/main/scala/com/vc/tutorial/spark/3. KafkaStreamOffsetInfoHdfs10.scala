package com.vc.tutorial.spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-8-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-10-integration.html
  * https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
  *
  * * Only Receiver Based Approach Until 1.3.0
  * https://spark.apache.org/docs/1.2.2/streaming-kafka-integration.html
  *
  * Receiver Based + Direct Stream Approach
  * https://spark.apache.org/docs/1.3.0/streaming-kafka-integration.html
  */
object KafkaStreamOffsetInfoHdfs10 {

  def main(args: Array[String]): Unit = {
    /**
      * https://spark.apache.org/docs/2.0.1/streaming-programming-guide.html#checkpointing
      * checkpointPath should be fault-tolerant, reliable file system (e.g., HDFS, S3, etc.)
      * For simplicity purpose it is assigned to local /checkpointDirectory
      *
      * StreamingContext.getOrCreate will first try to get StreamingContext instance from checkpoint directory(in case of recovering failure)
      * If not found it will create new instance(Start of application)
      * For Simplicity i have used local storage, it should be some path in hdfs
      **/
    val checkPointPath = s"/tmp/${getClass.getName}/checkPointDir"
    val ssc = StreamingContext.getOrCreate(
      checkPointPath,
      () => {
        println("creating new Instance")
        val sparkConf = new SparkConf().setAppName("KafkaStreamOffsetInfoHdfs10").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(2))
        ssc.checkpoint(checkPointPath)

        /**
          * Direct Stream Approach
          **/
        val kafkaParam = Map[String, Object](
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest",
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG  -> classOf[StringDeserializer],
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
          ConsumerConfig.GROUP_ID_CONFIG -> "KafkaStreamOffsetInfoHdfs10"
        )
        val topic = Array("test")
        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topic, kafkaParam)
        )
        stream.foreachRDD(rdd => {
          rdd.foreach(r => {
            println(s"partition: ${r.partition()} offset: ${r.offset()} value: ${r.value}")
          })
        })
        ssc
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
