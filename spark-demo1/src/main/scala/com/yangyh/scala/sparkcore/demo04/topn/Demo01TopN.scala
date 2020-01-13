package com.yangyh.scala.sparkcore.demo04.topn

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

/**
 * 分组取Top
 */
object Demo01TopN {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("分组取TopN")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/scores.txt")

    val rdd1: RDD[(String, Int)] = lines.map(line => {
      val words = line.split("\t")
      Tuple2(words(0), words(1).toInt)
    })

    val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()

    // 定长数组
    rdd2.foreach(tp => {
      val className = tp._1
      val iterator: Iterator[Int] = tp._2.iterator
      val top3 = new Array[Int](3)

      iterator.foreach(score => {

        val loop = new Breaks()
        loop.breakable {
          for (i <- 0 until top3.length) {
            if (top3(i) == 0) {
              top3(i) = score
              loop.break
            } else if (score > top3(i)) {
              for (j <- top3.length - 1 until(i, -1)) {
                top3(j) = top3(j - 1)
              }
              top3(i) = score
              loop.break
            }
          }
        }

      })
      println(s"className:$className,score:${util.Arrays.toString(top3)}")

    })

  }

}
