package com.yangyh.scala.sparkcore.demo02.rddfunc.demo02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * takeSample
 */
object Demo05TakeSample {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("top 算子").getOrCreate()
    val sc = session.sparkContext

    val rdd: RDD[String] = sc.parallelize(List[String]("a", "b", "c", "d", "e", "f"))

    /**
     * takeSample：从RDD数据集中随机获取几个
     * withReplacement:抽后是否放回,false不放回
     */
    val takeSampleResult = rdd.takeSample(false, 6)
    takeSampleResult.foreach(println)

  }


}
