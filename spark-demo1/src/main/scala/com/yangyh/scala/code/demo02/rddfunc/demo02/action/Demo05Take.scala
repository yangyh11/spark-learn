package com.yangyh.scala.code.demo02.rddfunc.demo02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * takeOrdered,takeSample
 */
object Demo05Take {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("top 算子").getOrCreate()
    val sc = session.sparkContext

    val rdd: RDD[String] = sc.parallelize(List[String]("a", "b", "c", "d", "e", "f"))

    /**
     * takeOrdered：对RDD中元素进行升序排列（字典序），获取前n个元素
     */
    val takeOrderedResult = rdd.takeOrdered(2)
    takeOrderedResult.foreach(println)

    /**
     * takeSample：从RDD数据集中随机获取几个
     * withReplacement:抽后是否放回,false不放回
     */
    val takeSampleResult = rdd.takeSample(false, 6)
    takeSampleResult.foreach(println)

  }


}
