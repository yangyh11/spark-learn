package com.yangyh.scala.code.demo07.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 将普通的RDD转换成DataFrame方式：
 * 1.反射方式
 *    需要自己定义类
 *    RDD[String] -> RDD[MyPerson] -> RDD[MyPerson].toDF
 * 2.动态创建Scheme
 *    RDD[String] -> RDD[Row] -> session.createDataFrame(RDD[Row], StructType)
 */

case class MyPerson(id: Int, name: String, age: Int, score: Double)

object Demo03Rdd2Df {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("demo03").getOrCreate()

    val peopleRdd : RDD[String] = session.sparkContext.textFile("./data/people.txt")

    /** 反射方式加载DataFrame */
    val personRdd: RDD[MyPerson] = peopleRdd.map(elem => {
      val args = elem.split(",")
      val id = args(0).toInt
      val name = args(1)
      val age = args(2).toInt
      val score = args(3).toDouble
      MyPerson(id, name, age, score)
    })

    import session.implicits._
    val df: DataFrame = personRdd.toDF()
//    df.show()

//    val df: Dataset[MyPerson] = personRdd.toDS()
//    df.show()


    /** 动态创建Scheme */
    val rowRdd: RDD[Row] = peopleRdd.map(elem => {
      val args = elem.split(",")
      val id = args(0).toInt
      val name = args(1)
      val age = args(2).toInt
      val score = args(3).toDouble
      Row(id, name, age, score)
    })

    val structType: StructType = StructType(List[StructField](
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("score", DoubleType, true)
    ))

    val df2: DataFrame = session.createDataFrame(rowRdd, structType)
    df2.show()

  }
}
