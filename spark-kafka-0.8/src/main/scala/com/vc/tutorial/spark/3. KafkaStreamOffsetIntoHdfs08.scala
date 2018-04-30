package com.vc.tutorial.spark

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-8-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-10-integration.html
  *
  * Only Receiver Based Approach Until 1.3.0
  * https://spark.apache.org/docs/1.2.2/streaming-kafka-integration.html
  *
  * Receiver Based + Direct Stream Approach
  * https://spark.apache.org/docs/1.3.0/streaming-kafka-integration.html
  */
object KafkaStreamOffsetIntoHdfs08 {
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
    println(checkPointPath)
    val ssc = StreamingContext.getOrCreate(
      checkPointPath,
      () => {
        println("creating new Instance")
        val sparkConf = new SparkConf().setAppName("KafkaStreamOffsetIntoHdfs08").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(2))
        ssc.checkpoint(checkPointPath)

        /**
          * Direct stream approach
          **/
        val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc,
          Map[String, String](
            "bootstrap.servers" -> "localhost:9092",
            "auto.offset.reset" -> "smallest"
          ),
          Set("test")
        )
        stream.foreachRDD(rdd => {
          rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(o => {
            println(s"partition: ${o.partition} offset: ${o.fromOffset} untilOffset: ${o.untilOffset}")
          })
          rdd.foreach(r => {
            println(s"value: ${r._2}")
          })
        })
        ssc
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
