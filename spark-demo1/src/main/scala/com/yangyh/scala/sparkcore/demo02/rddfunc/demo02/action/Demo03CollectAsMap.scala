package com.yangyh.scala.sparkcore.demo02.rddfunc.demo02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * collectAsMap 算子
 */
object Demo03CollectAsMap {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("collectAsMap").getOrCreate()
    val sc = session.sparkContext

    val rdd: RDD[(Int, String)] = sc.parallelize(List[(Int, String)](
      (1, "aa"),
      (2, "bb"),
      (3, "cc"),
      (4, "dd")
    ))


    /**
     * collectAsMap：对K,V格式的RDD数据转换成Map<K, V>
     */
    rdd.collectAsMap().foreach(println)
  }

}
