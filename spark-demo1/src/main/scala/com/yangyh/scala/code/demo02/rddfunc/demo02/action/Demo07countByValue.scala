package com.yangyh.scala.code.demo02.rddfunc.demo02.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * countByValue 算子
 */
object Demo07countByValue {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("countByValue 算子")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(Array[(String, Int)](
      ("a", 1),
      ("b", 2),
      ("a", 3),
      ("c", 4),
      ("d", 5)
    ))

    val rdd2: RDD[String] = sc.makeRDD(Array[String]("a", "b", "c", "d", "e", "f"))

    /**
     * countByValue算子：对RDD（KV格式或非KV格式）中的数据进行计数。
     */
    val result1: collection.Map[(String, Int), Long] = rdd1.countByValue()
    result1.foreach(println)

    val result2: collection.Map[String, Long] = rdd2.countByValue()
    result2.foreach(println)


  }

}
