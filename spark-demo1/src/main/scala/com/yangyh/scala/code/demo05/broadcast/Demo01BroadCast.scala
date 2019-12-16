package com.yangyh.scala.code.demo05.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量
 */
object Demo01BroadCast {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("广播变量")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array[String]("Java", "C", "Python", "Hadoop", "Spark"),2)
    // driver每发送一个task到executor，就将此变量发送给executor一次，占用executor内存
    val blackList = List("C", "Python")
    // 使用广播变量，driver将只发送一次到executor，在executor上管理
    val broadCast = sc.broadcast(blackList)

    val rdd2: RDD[String] = rdd1.filter(!broadCast.value.contains(_))

    rdd2.foreach(println)

  }

}
