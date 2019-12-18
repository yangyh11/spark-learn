package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * distinct 算子
 */
object Demo04Distinct {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().master("local").appName("distinct 算子").getOrCreate()
    val sc = session.sparkContext

    val rdd1 = sc.parallelize(List[Int](1, 1, 2, 3, 4, 5, 5))

    /**
     * distinct算子：
     * map + reduceByKey + map
     */
    val distinctResult: RDD[Int] = rdd1.distinct()
    distinctResult.foreach(println)


  }


}
