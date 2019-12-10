package com.yangyh.scala.code.demo02.rddfunc.demo02.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Action算子：行动算子
 */
object Day01Action {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Action 算子")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7))


    /** count算子 */
    val count = rdd1.count()
    println(count)

    /** take算子 */
    val takeResult = rdd1.take(5)
    takeResult.foreach(println)

    /** first算子 */
    val firstResult = rdd1.first()
    println(firstResult)

    /** collect算子 */
    val collect: Array[Int] = rdd1.collect()
    collect.foreach(println)

  }


}
