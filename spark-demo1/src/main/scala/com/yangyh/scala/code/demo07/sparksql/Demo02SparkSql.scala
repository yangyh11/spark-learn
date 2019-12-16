package com.yangyh.scala.code.demo07.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 读取Json格式的字符串加载DataFrame
 */
object Demo02SparkSql {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("demo02").getOrCreate()
    val list  = List[String](
      "{\"name\":\"zhangsan\",\"age\":\"18\"}",
      "{\"name\":\"lisi\",\"age\":\"19\"}",
      "{\"name\":\"wangwu\",\"age\":\"20\"}",
      "{\"name\":\"maliu\",\"age\":\"21\"}"
    )
    val rdd1: RDD[String] = session.sparkContext.parallelize(list)
    val df: DataFrame = session.read.json(rdd1)
//    df.show()
    
    import session.implicits._
    val jsonDS: Dataset[String] = list.toDS()
    val df2: DataFrame = session.read.json(jsonDS)
    df2.show()

  }

}
