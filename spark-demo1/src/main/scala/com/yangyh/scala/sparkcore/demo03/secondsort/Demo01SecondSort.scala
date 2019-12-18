package com.yangyh.scala.sparkcore.demo03.secondsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


case class MySort(first: Int, second: Int) extends Ordered[MySort] {
  override def compare(that: MySort): Int = {
    if(this.first == that.first) {
      this.second - that.second
    } else {
      this.first - that.first
    }
  }
}

/**
 * 二次排序
 */
object Demo01SecondSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("二次排序")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("./data/secondSort.txt")

    val rdd1: RDD[(MySort, String)] = lines.map(line => {
      val words = line.split(" ")
      Tuple2(MySort(words(0).toInt, words(1).toInt), line)
    })

    rdd1.sortByKey(false).foreach(elem => println(elem._2))

  }

}
