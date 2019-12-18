package com.yangyh.scala.sparkcore.demo02.rddfunc.demo01.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 * mapPartitions 算子
 */
object Demo06MapPartitions {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("mappartitions 算子").getOrCreate()
    val sc = session.sparkContext

    val rdd: RDD[Int] = sc.parallelize(List[Int](1, 2, 3, 4, 5), 2)

    /**
     * mapPartitions算子：与map类似，遍历的单位是每个partition
     */
    rdd.mapPartitions(iter => {
      val list = new ListBuffer[Int]
      println("创建连接")

//      iter.foreach(list.append(_))
      while(iter.hasNext) {
        list.append(iter.next())
      }

      println("数据插入 " + list.toString())
      println("关闭连接")
      println("====================")
      list.iterator
    }).count()

    // 使用map
//    rdd.map(elem => {
//      println("创建连接")
//      println("数据插入" + elem)
//      println("关闭连接")
//      println("====================")
//    }).count()



  }

}
