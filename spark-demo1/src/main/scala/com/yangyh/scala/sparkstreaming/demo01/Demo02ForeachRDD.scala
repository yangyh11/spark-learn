package com.yangyh.scala.sparkstreaming.demo01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * DStream的output operation类算子：foreachRDD算子
 * 1.可以获取DStream中的RDD，对RDD使用Transformation类算子进行转换，但是最后
 * 一定要在foreachRDD算子内使用RDD的action算子触发执行，否则RDD的Transformation类算子不会执行。
 * 2.foreachRDD算子内，获取的RDD的算子外的代码是在Driver端执行的。
 *   利用这个特点可以动态改变广播变量。
 */
object Demo02ForeachRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("ForeachRdd demo")

    val ssc: StreamingContext = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("error")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node4", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val pairWord: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = pairWord.reduceByKey(_ + _)

    result.foreachRDD(rdd => {
      println("************  在Driver端执行 ******************")

      val mapRdd: RDD[String] = rdd.map(tp => {
        println("----------  在Executor端执行 ------------------")
        tp._1 + "#" + tp._2
      })
      // Executor端执行的代码必须得使用action算子才能触发
      mapRdd.foreach(println)
    })

    ssc.start()
    // 等待Spark程序被终止
    ssc.awaitTermination()
    ssc.stop()

  }

}
