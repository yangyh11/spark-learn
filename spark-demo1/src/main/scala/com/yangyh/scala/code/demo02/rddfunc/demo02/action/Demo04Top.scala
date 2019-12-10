package com.yangyh.scala.code.demo02.rddfunc.demo02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * top 算子
 */
object Demo04Top {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("top 算子").getOrCreate()
    val sc = session.sparkContext

    val rdd: RDD[String] = sc.parallelize(List[String]("a", "b", "c", "d", "e", "f"))

    /**
     * top：对RDD中元素进行倒序排列（字典序），获取前n个元素
     */
    val topResult: Array[String] = rdd.top(2)
    topResult.foreach(println)

  }

}
