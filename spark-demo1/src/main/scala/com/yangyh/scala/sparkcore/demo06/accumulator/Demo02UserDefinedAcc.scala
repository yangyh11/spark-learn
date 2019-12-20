package com.yangyh.scala.sparkcore.demo06.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

case class PersonInfo(var personCount: Int, var ageCount: Int)

class MyAcc extends AccumulatorV2[PersonInfo, PersonInfo] {

  private var resultInfo = new PersonInfo(0, 0)

  /** 判断reset 是否是初始值，一定和reset保持一致 */
  override def isZero: Boolean = {
    resultInfo.personCount == 0 && resultInfo.ageCount == 0
  }

  override def copy(): AccumulatorV2[PersonInfo, PersonInfo] = {
    val myAcc = new MyAcc
    myAcc.resultInfo = this.resultInfo
    myAcc
  }

  override def reset(): Unit = {
    resultInfo = new PersonInfo(0, 0)
  }

  /** 作用在Executor端 */
  override def add(v: PersonInfo): Unit = {
    resultInfo.personCount += v.personCount
    resultInfo.ageCount += v.ageCount
  }

  /** Driver端的result与Executor中的每个result进行聚合 */
  override def merge(other: AccumulatorV2[PersonInfo, PersonInfo]): Unit = {
    resultInfo.personCount += other.asInstanceOf[MyAcc].resultInfo.personCount
    resultInfo.ageCount += other.asInstanceOf[MyAcc].resultInfo.ageCount
  }

  override def value: PersonInfo = resultInfo
}

/**
 * 用户自定义累加器
 */
object Demo02UserDefinedAcc {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("自定义累加器")

    val sc = new SparkContext(conf)

    sc.setLogLevel("error")


    // 自定义累加器
    val myAcc = new MyAcc
    sc.register(myAcc)

    val rdd1: RDD[String] = sc.parallelize(Array[String](
      "张三 18", "李四 20", "王五 21", "马六 16"
    ), 3)

    val value: RDD[Unit] = rdd1.map(elem => {
      val age = elem.split(" ")(1).toInt
      myAcc.add(new PersonInfo(1, age))
    })
    // action算子触发
    value.count()

    println("accumlator value = " + myAcc.value)

  }

}
