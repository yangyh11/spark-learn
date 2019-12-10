package com.yangyh.scala.code.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * union 算子
 */
object Demo03Union {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("union 算子").getOrCreate()
    val sc = session.sparkContext

    val rdd1 = sc.parallelize(List[Int](1, 2, 3, 4, 5, 6))
    val rdd2 = sc.parallelize(List[Int](4, 5, 6, 7, 8))

    /**
     * union算子：合并两个数据集，两个数据集的类型要一致
     * 返回新的RDD分区是原来两个合并的RDD分区数之和
     */
    val unionResult: RDD[Int] = rdd1.union(rdd2)
//    unionResult.foreach(println)

    /**
     * intersection算子：取两个数据集的交集 [4, 5, 6]
     *
     */
    val intersectionResult: RDD[Int] = rdd1.intersection(rdd2)
//    intersectionResult.foreach(println)

    /**
     * subtract算子：求两个数据集的差集 [1, 2, 3]
     */
    val subtractResult: RDD[Int] = rdd1.subtract(rdd2)
    subtractResult.foreach(println)




  }

}
