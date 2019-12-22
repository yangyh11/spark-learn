package com.yangyh.scala.sparkstreaming.demo01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * 从Socket读取数据
 */
object Demo01ReadSocket {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("SparkStreaming demo1")

    val ssc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node4", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val pairWord: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = pairWord.reduceByKey(_ + _)
    // DStream的outputoperator类算子
    result.print()

    ssc.start()
    // 等待Spark程序被终止
    ssc.awaitTermination()




  }

}
