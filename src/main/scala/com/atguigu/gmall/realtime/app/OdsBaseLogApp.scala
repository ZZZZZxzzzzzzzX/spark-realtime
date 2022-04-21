package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.utils.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import com.alibaba.fastjson._
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{Pagelog}

import java.lang

/**
 * 日志数据消费分流
 * 1. 准备实时环境
 * 2. 从kafka中消费数据
 * 3. 处理数据
 * 3.1 转换数据结构
 * 通用结构  Map 、 JSON 。。。
 * 专用结构  自己封装的Bean对象
 * 4. 将数据分流到kafka
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    //传入配置，和采集周期
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //2. 从kafka中消费数据
    val topicName: String = "ODS_BASE_LOG_1118"
    val group: String = "ODS_BASE_LOG_GROUP"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtils.getKafkaDstream(ssc, topicName, group)

    //3. 处理数据
    val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map(
      consumerRecord => {
        val jsonStr: String = consumerRecord.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    )
    //3.2数据分流
    //测试消费
    //    jsonObjDStream.print(100)
    /*
      分流原则：
      1. 启动数据 -> DWD_START_TOPIC_1118
      2. 页面日志数据
          2.1 页面数据 -> DWD_PAGE_LOG_TOPIC_1118
          2.2 曝光数据 -> DWD_DISPLAY_TOPIC_1118
          2.3 事件数据 -> DWD_ACTION_TOPIC_1118
      3. 错误数据   错误数据不做任何拆分处理，整条数据直接写入到DWD_REEOR_TOPIC_1118
      4. 所有的数据都要与公共字段进行组合，完整的写入到对应的主题中
     */

    val error_topic: String = "DWD_ERROR_TOPIC_1118"
    val start_topic: String = "DWD_START_TOPIC_1118"
    val page_topic: String = "DWD_PAGE_LOG_TOPIC_1118"
    val display_topic: String = "DWD_DISPLAY_TOPIC_1118"
    val action_topic: String = "DWD_ACTION_TOPIC_1118"
    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonObj => {
            //对一条数据分流
            //分流错误数据
            val errObj: JSONObject = jsonObj.getJSONObject("err")
            if (errObj != null) {
              //直接写入到错误主题中
              MyKafkaUtils.send(error_topic, jsonObj.toJSONString)
            } else {
              //提取common
              val commonObj: JSONObject = jsonObj.getJSONObject("common")
              val ar: String = commonObj.getString("ar")
              val uid: String = commonObj.getString("uid")
              val os: String = commonObj.getString("os")
              val ch: String = commonObj.getString("ch")
              val isNew: String = commonObj.getString("is_new")
              val md: String = commonObj.getString("md")
              val mid: String = commonObj.getString("mid")
              val vc: String = commonObj.getString("vc")
              val ba: String = commonObj.getString("ba")

              //提取ts
              val ts: lang.Long = jsonObj.getLong("ts")

              //分流页面日志数据
              val pageObj: JSONObject = jsonObj.getJSONObject("page")
              if(pageObj != null) {
                val pageId: String = pageObj.getString("page_id")
                val pageItem: String = pageObj.getString("item")
                val pageItemType: String = pageObj.getString("item_type")
                val duringTime: Long = pageObj.getLong("during_time")
                val lastPageId: String = pageObj.getString("last_page_id")
                val sourceType: String = pageObj.getString("source_type")

                //封装成Paglog
                val pageLog =
                  Pagelog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,ts)
                //写入到对应的主题中
                MyKafkaUtils.send(page_topic, JSON.toJSONString(pageLog, new SerializeConfig(true)))
              }
            }
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
