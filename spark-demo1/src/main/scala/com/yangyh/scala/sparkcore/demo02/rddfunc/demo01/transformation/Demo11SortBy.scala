package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sortBy算子
 * 1.对于单列数据，按照本身进行排序。
 * 2.对于多元组数据，可以指定以第几个元素进行排序。
 */
object Demo11SortBy {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sortBy算子").setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("error")

    /** 1.对于单列数据，按照本身进行排序。 */
    val rdd1: RDD[Int] = sc.parallelize(List(1, 4, 5, 7, 6, 9, 4, 3))
    rdd1.sortBy(i => i).foreach(println)

    println("=====================================")

    /** 2.对于多元组数据，可以指定以第几个元素进行排序。 */
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a", 10), ("c", 3), ("g", 8), ("h", 6),("f",10),("b",10)))
    // 按照第二个元素进行倒叙排序
    rdd2.sortBy(_._2,false).foreach(println)

    println("=====================================")

    // 先按照第二个元素倒叙排序，如果第二个元素相同就按照第一个元素倒叙排序
    rdd2.sortBy(elem => (elem._2,elem._1),false).foreach(println)

  }

}
