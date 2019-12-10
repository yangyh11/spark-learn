package com.yangyh.java.code.demo02.rddfunc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @description: 转换算子
 * @author: yangyh
 * @create: 2019-12-09 19:39
 */
public class Demo01Transformation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("transformation");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        /** filter:过滤算子 */
        JavaRDD<Integer> filter = rdd1.filter(num -> num > 3);
        filter.foreach(elem -> System.out.println(elem));

        /** sample：抽样算子 */
        JavaRDD<Integer> sample = rdd1.sample(true, 0.1);
        sample.foreach(elem -> System.out.println(elem));

    }
}
