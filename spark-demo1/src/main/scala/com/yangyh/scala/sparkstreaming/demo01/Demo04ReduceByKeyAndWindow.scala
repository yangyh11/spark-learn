package com.yangyh.scala.sparkstreaming.demo01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * reduceByKeyAndWindow 窗口操作：
 * 每隔滑动间隔按照给定的逻辑计算最近窗口长度内的数据。
 * 注意：
 *  windowlength和slidinginterval必须是batchInterval的整数倍
 */
object Demo04ReduceByKeyAndWindow {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("ReduceByKeyAndWindow demo")

    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("error")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node4", 9999)

    val pairWords: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    /** 普通机制 */
    val result: DStream[(String, Int)] = pairWords.reduceByKeyAndWindow((v1: Int, v2: Int) => {
      v1 + v2
    }, Durations.seconds(15), Durations.seconds(5))

    result.print()

    /** 优化机制 */
    ssc.checkpoint("./data/streamingCheckpoint")
    val windowResult: DStream[(String, Int)] = pairWords.reduceByKeyAndWindow(
      (v1:Int, v2:Int)=>{v1+v2},
      (v1:Int, v2:Int)=>{v1-v2},
      Durations.seconds(15),
      Durations.seconds(5))
    windowResult.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
