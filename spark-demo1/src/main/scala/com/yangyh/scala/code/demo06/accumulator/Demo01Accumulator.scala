package com.yangyh.scala.code.demo06.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 累加器
 * 累加器相当于分布式中的统筹变量，用于分布式统计
 * 累加器在Driver端定义，在Executor中更新
 */
object Demo01Accumulator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("累加器")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array[String]("Java", "C", "Python", "Hadoop", "Spark"),2)
    // 结果为0
    var sum = 0
    // 使用累加器
    val accumulator = sc.longAccumulator

    rdd1.foreach(_ => {
      sum += 1
      println(s"Executor -- i = $sum")
      accumulator.add(1)
      println(s"Executor -- accumulator = $accumulator")

    })
    println(s"sum:$sum")
    println(s"accumulator:${accumulator.value}")
  }

}
