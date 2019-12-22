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
    // receive模式下，local的模拟线程必须等于大于等于2，一个线程用来receive接收数据，一个线程用来处理数据
    conf.setMaster("local[2]")
    conf.setAppName("SparkStreaming demo1")

    // batchDuration：流数据被分成批次的时间间隔。每隔5s就将这5s时间接受到的数据保存到一个batch中。
    val ssc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node4", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val pairWord: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = pairWord.reduceByKey(_ + _)
    // DStream的output operation类算子
    result.print()

    ssc.start()
    // 等待Spark程序被终止
    ssc.awaitTermination()
    ssc.stop()




  }

}
