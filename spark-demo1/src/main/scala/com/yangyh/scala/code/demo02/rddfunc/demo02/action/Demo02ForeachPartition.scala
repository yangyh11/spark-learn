package com.yangyh.scala.code.demo02.rddfunc.demo02.action

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 * foreachPartition
 */
object Demo02ForeachPartition {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("foreachPartition 算子").getOrCreate()
    val sc = session.sparkContext

    val rdd = sc.parallelize(List[Int](1, 2, 3, 4, 5), 2)

    /**
     * foreachPartition算子：与foreach类似，遍历的单位是每个partition
     */
    rdd.foreachPartition(iter => {
      println("创建连接")

      val list = new ListBuffer[Int]
      iter.foreach(list.append(_))

      println("数据插入 " + list.toString())
      println("关闭连接")
      println("====================")
    })


//    rdd.foreach(elem => {
//      println("创建连接")
//      println("数据插入" + elem)
//      println("关闭连接")
//      println("====================")
//    })

  }

}
