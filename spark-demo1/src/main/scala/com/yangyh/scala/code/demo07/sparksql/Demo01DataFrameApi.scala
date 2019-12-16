package com.yangyh.scala.code.demo07.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 1.DataFrame是一个一个Row型的RDD。
 * 2.两种方式读取Json格式的的文件。
 * 3.df.show()默认显示前20行。
 * 4.DataFrame原生API可以操作DataFrame。
 * 5.注册成临时表时，表中的列默认按ascii顺序显示。
 */
object Demo01DataFrameApi {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").appName("spark sql").getOrCreate()

    val df: DataFrame = session.read.json("./data/json")

    df.printSchema()
    // 默认打印前20条
//    df.show()
//    frame.show(10)

    /** select name,age from table where age >= 19 **/
    val df2: Dataset[Row] = df.select("name", "age").where(df.col("age").>=(19))
//    df2.show()

    /** select * from table where age >= 19 **/
    val df3: Dataset[Row] = df.filter("age >= 19")
//    df3.show()

    /** select name,age+10 as addage from table */
    val df4: DataFrame = df.select(df.col("name"), df.col("age").plus(10).alias("addage"))
//    df4.show()

    /** select name,age from table order by name ,age desc */
    import session.implicits._
    val df5: Dataset[Row] = df.sort($"name".asc, $"age".desc)
//    df5.show()

    /** 注册一个视图，直接使用sql语句 */
    df.createTempView("myTable")
    val df6: DataFrame = session.sql("select name,age from myTable where age = 18")
    df6.show()

  }

}
