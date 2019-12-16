package com.yangyh.java.code.demo03.secondsort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-12-13 11:02
 */
public class Demo01SecondSort {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("secondSort");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("./data/secondSort.txt");

        JavaPairRDD<MySort, String> mySortStringJavaPairRDD = lines.mapToPair(line -> {
            String[] array = line.split(" ");
            Integer first = Integer.valueOf(array[0]);
            Integer second = Integer.valueOf(array[1]);
            return new Tuple2<MySort, String>(new MySort(first, second), line);
        });

        mySortStringJavaPairRDD.sortByKey(false).foreach(tp -> System.out.println(tp._2));

    }
}
