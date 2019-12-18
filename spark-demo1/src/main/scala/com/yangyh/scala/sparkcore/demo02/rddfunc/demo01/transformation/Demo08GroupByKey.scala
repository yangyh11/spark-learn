package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey 算子
 */
object Demo08GroupByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("groupByKey 算子")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(Array[(String, Int)](
      ("aa", 1),
      ("bb", 1),
      ("aa", 2),
      ("cc", 1),
      ("dd", 4)
    ))

    /**
     * groupByKey算子：分组，作用在K，V格式的RDD的上
     */
    val result: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    result.foreach(println)

  }

}
