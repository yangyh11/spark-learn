package com.yangyh.scala.code.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * join，leftOuterJoin，rightOuterJoin，fullOuterJoin 算子
 */
object Demo02Join {

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

    /** join算子 */
    val joinResult: RDD[(String, (Int, Int))] = ageRdd.join(scoreRdd)
//    joinResult.foreach(println)

    /** leftOuterJoin */
    val leftOuterJoinResult: RDD[(String, (Int, Option[Int]))] = ageRdd.leftOuterJoin(scoreRdd)
//    leftOuterJoinResult.foreach(println)

    /** rightOuterJoin */
    val rightOuterJoin: RDD[(String, (Option[Int], Int))] = ageRdd.rightOuterJoin(scoreRdd)
//    rightOuterJoin.foreach(println)

    /** fullOuterJoin */
    val fullOuterJoinResult: RDD[(String, (Option[Int], Option[Int]))] = ageRdd.fullOuterJoin(scoreRdd)
    fullOuterJoinResult.foreach(println)


  }

}
