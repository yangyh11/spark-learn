package com.yangyh.java.code.demo01.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-12-06 22:08
 */
public class WordCount {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf();
        conf.setAppName("wordCount");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words");
        JavaRDD<String> words = lines.flatMap(line -> {
            String[] wordArray = line.split(" ");
            return Arrays.asList(wordArray).iterator();
        });

        JavaPairRDD<String, Integer> pairWords = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> result = pairWords.reduceByKey((v1, v2) -> v1 + v2);

        result.foreach(value -> System.out.println(value));


    }
}
