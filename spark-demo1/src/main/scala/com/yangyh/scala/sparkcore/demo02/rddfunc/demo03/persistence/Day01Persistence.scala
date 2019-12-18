package com.yangyh.scala.sparkcore.demo02.rddfunc.demo03.persistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 持久化算子
 */
object Day01Persistence {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("persistence 算子")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/persistData.txt")

    /** 1.cache算子 */
    // 持久化到了内存。cache() = persist() = persist(StorageLevel.MEMORY_ONLY)
    //    lines.cache()

    /** 2.persist算子 */
    // 持久化到了内存。默认persist()就是持久化到内存中
    lines.persist(StorageLevel.MEMORY_ONLY)

    // 第一次是从磁盘读
    val startTime1 = System.currentTimeMillis()

    val total1 = lines.count()

    val endTime1 = System.currentTimeMillis()
    val time1 = endTime1 - startTime1
    println(s"total:$total1,time: $time1 ms")


    // 第二次是从持久化的地方读
    val startTime2 = System.currentTimeMillis()

    val total2 = lines.count()

    val endTime2 = System.currentTimeMillis()
    val time2 = endTime2 - startTime2
    println(s"total:$total2,time: $time2 ms")

  }

}
