package com.yangyh.scala.code.demo02.rddfunc.demo04.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 算子练习
 * 思考：一千万条数据量的文件，过滤掉出现次数最多的记录，并且其余记录按照出现次数降序排序。
 */
object Demo05Practice {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("practice")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/words")

    // 持久化
//    lines.cache()

    val words = lines.flatMap(_.split("\\s"))

    // 简化版
    val filterWord = countAndSortDesc(words.sample(true, 0.4)).first()._1
    countAndSortDesc(words.filter(!filterWord.equals(_))).foreach(println)




    //    // 抽样
    //    val sampleResult = words.sample(true, 0.5)
    //
    //    // 抽样之后排序
    //    val sortResult = Demo04Practice.countAndSort(sampleResult)
    //
    //    val firstWords = sortResult.first()._1
    //
    //    // 过滤
    //    val filterResult = words.filter(!firstWords.equals(_))
    //
    //    // 过滤之后排序
    //    val result = Demo04Practice.countAndSort(filterResult)
    //
    //    result.foreach(println)
  }

  /**
   * 单词计数并倒序
   *
   * @param rdd1
   * @return
   */
  def countAndSortDesc(rdd1: RDD[String]): RDD[(String, Int)] = {
    rdd1.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
  }

}
