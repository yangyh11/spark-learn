package com.yangyh.scala.sparkcore.demo02.rddfunc.demo03.persistence

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 持久化算子：checkPoint
 */
object Day02CheckPoint {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("checkPoint")

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("./data/ck")

    val rdd1 = sc.parallelize(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))



    rdd1.checkpoint()

    rdd1.count()



  }

}
