package com.yangyh.scala.code.demo02.rddfunc.demo04.practice

import org.apache.spark.{SparkConf, SparkContext}

object Demo02PvAndUv {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("pv and uv 算子")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/pvuvdata")

    /**
     * pv:即页面浏览量或点击量
     * 66.25.48.99	甘肃	2019-12-11	1576065469303	335845592107020436	www.jd.com	Click
     */
    lines.map(line => Tuple2(line.split("\t")(5), 1)).reduceByKey(_ + _).foreach(println)

    /**
     * uv:即独立访客数
     * 66.25.48.99	甘肃	2019-12-11	1576065469303	335845592107020436	www.jd.com	Click
     */
    lines.map(line => {
      val words = line.split("\t")
      Tuple2(words(5), words(0))
    }).distinct()
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
      .sortBy(_._2,false)
      .foreach(println)

  }

}
