package com.vc.tutorial.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val df = sc.textFile(getClass.getResource("/wordCount").getPath)
    val wordCount = df.flatMap(lines=>lines.split(" ")).map(word=>(word, 1)).reduceByKey((x,y)=> x + y)
    wordCount.saveAsTextFile("WordCountOutput")
  }
}
