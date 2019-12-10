package com.yangyh.java.code.demo02.rddfunc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @description: Action算子
 * @author: yangyh
 * @create: 2019-12-09 19:49
 */
public class Demo02Action {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Action算子");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        /** count:统计算子 */
        long total = rdd1.count();
        System.out.println(total);

        /** take算子 */
        List<Integer> take = rdd1.take(3);
        System.out.println(take);

        /** first算子 */
        Integer first = rdd1.first();
        System.out.println(first);

        /** connect算子*/
        List<Integer> collect = rdd1.collect();
        collect.forEach(elem -> System.out.println(elem));

    }
}
