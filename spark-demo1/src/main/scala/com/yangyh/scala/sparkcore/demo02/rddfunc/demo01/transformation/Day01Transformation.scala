package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Transformation算子：转换算子
 */
object Day01Transformation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("RDD Func")

    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(1, 1, 3, 4, 5, 6, 7, 8))

    /** filter算子 */
    val filterResult = rdd1.filter(_ % 2 == 0)
//    filterResult.foreach(println)

    /** sample算子 */
//    val rdd2 = sc.textFile("./data/words")
        val sampleResult = rdd1.sample(true, 0.5)
    // 每次结果都一样
//    val sampleResult = rdd2.sample(true, 0.1, 100L)
    println("=====================================")
    sampleResult.foreach(println)
  }

}
