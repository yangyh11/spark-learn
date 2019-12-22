package com.yangyh.scala.sparkstreaming.demo02.kafka

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.mutable

/**
  * 利用redis 来维护消费者偏移量
 * http://spark.apache.org/docs/2.3.1/streaming-kafka-0-10-integration.html
  */
object Demo03ManageOffsetUseRedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("ManageOffsetUseRedis")
    //设置每个分区每秒读取多少条数据
    conf.set("spark.streaming.kafka.maxRatePerPartition","10")

    val ssc = new StreamingContext(conf,Durations.seconds(5))
    //设置日志级别
    ssc.sparkContext.setLogLevel("Error")

    val topic = "myTopic"

    /**
      * 1.从Redis中获取消费者offset
      */
    val dbIndex = 3
    val currentTopicOffset: mutable.Map[String, String] = getOffSetFromRedis(dbIndex,topic)
    //初始读取到的topic offset:
    currentTopicOffset.foreach(tp=>{println(s" 初始读取到的offset: $tp")})

    //转换成需要的类型
    val fromOffsets: Predef.Map[TopicPartition, Long] = currentTopicOffset.map { resultSet =>
      // topic,partition,offset
      new TopicPartition(topic, resultSet._1.toInt) -> resultSet._2.toLong
    }.toMap

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,node2:9092,node3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MyGroupId11",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)//默认是true
    )

    /**
      * 2.将获取到的消费者offset传递给SparkStreaming,从指定的offset开始消费
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )

    stream.foreachRDD { (rdd:RDD[ConsumerRecord[String, String]]) =>

      /** 3.消费到消息后，进行相应的业务逻辑处理 */
      println("**** 业务处理完成  ****")

      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"topic:${o.topic}  partition:${o.partition}  fromOffset:${o.fromOffset}  untilOffset: ${o.untilOffset}")
      }

      /** 4.将当前批次最后的所有分区offsets 保存到 Redis中。更新redis中的offset */
      saveOffsetToRedis(dbIndex,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

  /**
    * 将消费者offset 保存到 Redis中
    *
    */
  def saveOffsetToRedis(db:Int,offsetRanges:Array[OffsetRange]) = {
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    offsetRanges.foreach(one=>{
      jedis.hset(one.topic, one.partition.toString,one.untilOffset.toString)
    })
    println("保存成功")
    RedisClient.pool.returnResource(jedis)
  }


  /**
    * 从Redis中获取保存的消费者offset
    * @param db
    * @param topic
    * @return
    */
  def getOffSetFromRedis(db:Int,topic:String)  ={
    val jedis = RedisClient.pool.getResource
    jedis.select(db)
    val result: util.Map[String, String] = jedis.hgetAll(topic)
    RedisClient.pool.returnResource(jedis)
    if(result.size()==0){
      result.put("0","0")
      result.put("1","0")
      result.put("2","0")
    }
    import scala.collection.JavaConversions.mapAsScalaMap
    val offsetMap: scala.collection.mutable.Map[String, String] = result
    offsetMap
  }
}
