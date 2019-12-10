package com.yangyh.scala.code.demo02.rddfunc.demo04.practice

import org.apache.spark.{SparkConf, SparkContext}

object Demo1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("RDD Func")

    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(1, 1, 3, 4, 5, 6, 7, 8, 3, 4, 5, 6))

    rdd1.map((_, 1)).reduceByKey((_, _) => {1}).map(_._1)foreach(println)


  }

}
