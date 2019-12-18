package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * cogroup 算子
 */
object Demo05Cogroup {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("join 算子").master("local").getOrCreate()
    val sc = session.sparkContext

    val ageRdd = sc.parallelize(List[(String, Int)](
      ("张三", 18),
      ("李四", 15),
      ("王五", 18),
      ("赵六", 19),
      ("孙琪", 20)
    ))

    val scoreRdd = sc.parallelize(List[(String, Int)](
      ("张三", 60),
      ("李四", 80),
      ("王五", 100),
      ("赵六", 120),
      ("田七", 20)
    ))

    /**
     * cogroup算子
     * 当调用类型(K,V)和(K,W)的数据时，返回一个数据集(K,(Iterable<V>, Iterable<W>))
     * 子RDD的分区与父RDD多的一致
     */
    val cogroupResult: RDD[(String, (Iterable[Int], Iterable[Int]))] = ageRdd.cogroup(scoreRdd)
    cogroupResult.foreach(println)

  }

}
