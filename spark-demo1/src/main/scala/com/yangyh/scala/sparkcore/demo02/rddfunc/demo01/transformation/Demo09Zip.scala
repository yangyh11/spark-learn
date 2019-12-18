package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * zip，zipWithIndex 算子
 */
object Demo09Zip {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("zip 算子")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array[(String, Int)](
      ("a", 1),
      ("b", 2),
      ("c", 3),
      ("d", 4),
      ("a", 5)
    ))

    val rdd2 = sc.makeRDD(Array[(String, Int)](
      ("aa", 1),
      ("bb", 2),
      ("cc", 3),
      ("dd", 4),
      ("aa", 5)
    ))


    /**
     * zip算子：将两个RDD中的元素（KV格式或非KV格式）变成一个KV格式的RDD
     * 两个RDD的每个分区元素个数必须相等
     */
    val result1: RDD[((String, Int), (String, Int))] = rdd1.zip(rdd2)
//    result1.foreach(println)


    /**
     * zipWithIndex算子：将RDD中的元素和这个元素在RDD中的索引号组合成(K,V)对,返回新的RDD
     */
    val result2: RDD[((String, Int), Long)] = rdd1.zipWithIndex()
    result2.foreach(println)

  }

}
