package com.yangyh.scala.code.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * mapPartitionsWithIndex,repartition,coalesce 算子
 */
object Demo07Repartition {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("rePartition 算子")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array[String](
      "aaa1", "aaa2", "aaa3", "aaa4",
      "aaa5", "aaa6", "aaa7", "aaa8",
      "aaa9", "aaa10", "aaa11", "aaa12"
    ), 3)

    /**
     * mapPartitionsWithIndex 算子：类似于mapPartitions，此外还会携带分区的索引值
     */
    val rdd2: RDD[String] = rdd1.mapPartitionsWithIndex((index, iter) => {
      val list = new ListBuffer[String]
      iter.foreach(value => list.append("rdd1 index【" + index + "】,value【" + value + "】"))
      list.iterator
    })

    /**
     * repartition 算子：对RDD进行重新分区，分区数可增可减
     * 会产生shuffle，底层就是coalesce(numPartitions, shuffle = true)
     */
    val repartitionRdd = rdd2.repartition(4)

    val repartitionResult = repartitionRdd.mapPartitionsWithIndex((index, iter) => {
      val list = new ListBuffer[String]
      iter.foreach(value => list.append("repartitionRdd index【" + index + "】" + value))
      list.iterator
    })

//    repartitionResult.collect().foreach(println)

    /**
     * coalesce 算子：对RDD进行重新分区，分区数可增可减。
     * 默认是不会产生shuffle。当分区数增多，同时没有指定要产生shuffle时，该算子不起作用。
     */
//    val coaleascRdd = rdd2.coalesce(4)
//    val coaleascRdd = rdd2.coalesce(4, true)
    val coaleascRdd = rdd2.coalesce(2)


    val coaleascResult = coaleascRdd.mapPartitionsWithIndex((index, iter) => {
      val list = new ListBuffer[String]
      iter.foreach(value => list.append("coaleascRdd index【" + index + "】" + value))
      list.iterator
    })

    coaleascResult.collect().foreach(println)

  }


}
