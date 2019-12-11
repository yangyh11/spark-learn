package com.yangyh.scala.code.demo02.rddfunc.demo02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * countByKey 算子
 */
object Demo06countByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("countByKey 算子")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(Array[(String, Int)](
      ("a", 1),
      ("b", 2),
      ("a", 3),
      ("c", 4),
      ("d", 5)
    ))


    /**
     * countByKey算子：对KV格式的RDD，根据K进行计数。返回的是一个Map
     */
    val result: collection.Map[String, Long] = rdd.countByKey()

    result.foreach(println)

  }



}
