package com.yangyh.scala.code.demo01.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    /**
     * SparkConf：
     * 1.SparkConf:设置Spark的配置
     *  运行模式：
     *    local：多用于本地的Eclipse、IDEA开发Spark程序使用
     *    standalone：是Spark自带的资源调度框架，支持分布式的搭建。
     *    Yarn：是Hadoop生态圈中的资源调度框架，Spark也可以基于Yarn调度
     *    Mesos：资源调度框架
     * 2.conf可以设置Spark application webui中的名称。
     * 3.conf可以设置Spark运行资源参数 -- 内存 + core
     */

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wordCount")

    /**
     * SparkContext是Spark上下文，通往集群的唯一通道
     */
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/words")
    val words: RDD[String] = lines.flatMap(line => {line.split(" ")})
    val pairWords: RDD[(String, Int)] = words.map(word => {
      new Tuple2[String, Int](word, 1)
    })
    val reduceResult: RDD[(String, Int)] = pairWords.reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })

    // 默认是升序
//    val result = reduceResult.sortBy(_._2)
    // 降序
    val result = reduceResult.sortBy(_._2, false)

//    val swapResult = reduceResult.map(_.swap)
//    val sortValue = swapResult.sortByKey()
//    val result = sortValue.map(_.swap)

    result.foreach(println)


    // 简写版
//    val sc = new SparkContext(conf)
//    val lines = sc.textFile("./data/words")
//    val words = lines.flatMap(_.split(" "))
//    val pairWords = words.map((_, 1))
//    val result = pairWords.reduceByKey(_ + _)
//    result.foreach(println)

    // 一行代码解决
//    new SparkContext(conf).textFile("./data/words").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)

  }

}
