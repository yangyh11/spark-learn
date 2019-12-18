package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapValues 算子
 */
object Demo10MapValues {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("mapValues 算子")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array[(String, Int)](
      ("a", 1),
      ("b", 2),
      ("c", 3),
      ("d", 4)
    ))

    /**
     * mapValues算子：对K，V格式的RDD中的value进行操作，返回的是KV格式的RDD
     */
    val result: RDD[(String, Int)] = rdd.mapValues(value => {
      value + 100
    })

    result.foreach(println)

  }

}
