package com.yangyh.scala.sparkstreaming.demo01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * 根据Key进行分组，然后对每组的value进行处理
 * 需要设置checkpoint目录用于保存状态，自从SparkStreaming启动以来将所有的Key的状态统计。
 */
object Demo03UpdateStateByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("UpdateStateByKey demo")

    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("error")
    ssc.checkpoint("./data/ck/sparkSteaming")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node4", 9999)

    val pairWords: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    val result: DStream[(String, Int)] = pairWords.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      var preValue = option.getOrElse(0)
      seq.foreach(elem => preValue += elem)
      Option(preValue)
    })
    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
