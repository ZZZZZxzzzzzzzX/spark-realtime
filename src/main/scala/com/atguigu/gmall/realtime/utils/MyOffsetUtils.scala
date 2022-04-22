package com.atguigu.gmall.realtime.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

/**
 *  偏移量管理工具
 */
object MyOffsetUtils {

  /**
   * 将偏移量写入到redis中
   *
   * Kafka中偏移量的结构：
   *      group + topic + partition （GTP） => offset
   *
   * redis中如何储存偏移量数据：
   *    类型：hash
   *    key：offset：topic：group
   *    value：partition-offset ......
   *    写入API：hset
   *    读取API：hgetall
   *    是否过期；不过期
   */
  def saveOffset(topic : String, group : String,offsetRanges: Array[OffsetRange]): Unit ={
    if(offsetRanges != null && offsetRanges.length > 0){
      val offsetValues = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition: Int = offsetRange.partition
        val endOffset: Long = offsetRange.untilOffset
        offsetValues.put(partition.toString, endOffset.toString)
      }
      println("写入偏移量： " + offsetValues)
      //存储到redis中
      //获取连接
      val jedis: Jedis = MyRedisUtils.getJedis()
      val offsetKey : String = s"offset:$topic$group"
      jedis.hset(offsetKey, offsetValues)
      MyRedisUtils.close(jedis)
    }
  }

  /**
   * 从redis中读取偏移量
   */
  def readOffset(topic : String, group : String): Map[TopicPartition,Long ] ={
    val jedis: Jedis = MyRedisUtils.getJedis()
    val offsetKey : String = s"offset:$topic$group"
    val partitionOffset: util.Map[String, String] = jedis.hgetAll(offsetKey)
    val offsets: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    println("读取到offset: " + partitionOffset)
    if(partitionOffset != null && partitionOffset.size() > 0) {
      import scala.collection.JavaConverters._
      for ((partition, offset) <- partitionOffset.asScala) {
        val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
        offsets.put(topicPartition, offset.toLong)
      }
    }
    MyRedisUtils.close(jedis)
    offsets.toMap
  }
}
