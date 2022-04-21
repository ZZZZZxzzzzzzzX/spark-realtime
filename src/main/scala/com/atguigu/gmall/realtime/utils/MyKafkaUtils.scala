package com.atguigu.gmall.realtime.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import java.util.concurrent.Future
import scala.collection.mutable

/**
 * Kafka的工具类，提供基于Kafka的生产者和消费功能
 */
object MyKafkaUtils {
  /**
   * 消费者的配置
   */
  val consumerParams: mutable.Map[String, Object] = mutable.Map[String, Object](
    //Kafka集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils.apply(MyConfig.KAFKA_BOOTSTRAP_SERVER),
    //kv的反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //Offset的提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //Offset的自动提交间隔
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000",
    //Offset重置
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  /**
   * 从Kafka中消费数据
   * 使用默认维护的offset消费
   * 基于SparkStreaming获取到DStream对象
   * @return
   */
  def getKafkaDstream(ssc: StreamingContext, topic: String, group: String) = {
    //动态指定消费者组
    consumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerParams)
    )
    kafkaDstream
  }

  /**
   * 从Kafka中消费数据
   * 指定offset进行消费
   */
//  def getKafkaDStream(ssc : StreamingContext , topic : String  , group: String, offsets: Map[TopicPartition,Long ] ) = {
//
//  }

  /**
   * 生产者对象
   */
  val kafkaProducer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   */
  def createProducer(): KafkaProducer[String, String] = {
    /**
     * 生产者配置
     */
    val producerParams: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    //集群位置
    producerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVER))
    //kv序列化器
    producerParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerParams)
    producer
  }

  /**
   * 往kafka中生产数据
   */
  def send(topic: String, msg: String) = {
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }
  def send(topic : String ,key: String ,  msg : String ) = {
    kafkaProducer.send(new ProducerRecord[String,String ](topic,key ,msg))
  }

  /**
   * 刷写
   */
  def flush(): Unit ={
    if (kafkaProducer != null) kafkaProducer.flush()
  }
}
