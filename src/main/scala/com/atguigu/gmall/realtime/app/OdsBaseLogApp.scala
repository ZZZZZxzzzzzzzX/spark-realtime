package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.utils.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import com.alibaba.fastjson._
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{PageActionLog, PageDisplayLog, Pagelog, StartLog}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

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

    //TODO 2补充： 指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, group)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, group,offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDstream(ssc, topicName, group)
    }

    //TODO 1补充：提取偏移量
    var offsetRanges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //3. 处理数据
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map(
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
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
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

                    //分流曝光数据
                    val displayJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                    if(displayJsonArr !=null && displayJsonArr.size() > 0){
                      for(i <- 0 until displayJsonArr.size()){
                        val displayObj: JSONObject = displayJsonArr.getJSONObject(i)
                        //提取曝光字段
                        val displayType: String = displayObj.getString("display_type")
                        val displayItem: String = displayObj.getString("item")
                        val displayItemType: String = displayObj.getString("item_type")
                        val order: String = displayObj.getString("order")
                        val posId: String = displayObj.getString("pos_id")
                        val pageDisplayLog =
                          PageDisplayLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,displayType,displayItem,displayItemType,order,posId,ts)
                        //发送到对应的主题
                        MyKafkaUtils.send(display_topic , JSON.toJSONString(pageDisplayLog , new SerializeConfig(true)))
                      }
                    }
                    //分流动作数据
                    val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                    if(actionJsonArr != null && actionJsonArr.size()>0){
                      for(i <- 0 until actionJsonArr.size()){
                        val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                        //提取动作字段
                        val actionId: String = actionObj.getString("action_id")
                        val actionItem: String = actionObj.getString("item")
                        val actionItemType: String = actionObj.getString("item_type")
                        val actionTs: lang.Long = actionObj.getLong("ts")

                        //封装成PageActionLog
                        val pageActionLog =
                          PageActionLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,actionId,actionItem,actionItemType,actionTs ,ts)
                        //发送到对应的主题中
                        MyKafkaUtils.send(action_topic , JSON.toJSONString(pageActionLog,new SerializeConfig(true)))

                      }
                    }
                    //分流启动数据
                    val startObj: JSONObject = jsonObj.getJSONObject("start")
                    if(startObj != null ){
                      val entry: String = startObj.getString("entry")
                      val loadingTime: lang.Long = startObj.getLong("loading_time")
                      val openAdId: String = startObj.getString("open_ad_id")
                      val openAdMs: Long = startObj.getLong("open_ad_ms")
                      val openAdSkipMs: Long = startObj.getLong("open_ad_skip_ms")

                      //封装成StartLog
                      val startLog =
                        StartLog(mid,uid,ar,ch,isNew,md,os,vc,ba,entry,openAdId,loadingTime,openAdMs,openAdSkipMs,ts)
                      //发送到对应的主题
                      MyKafkaUtils.send(start_topic , JSON.toJSONString(startLog,new SerializeConfig(true)))
                    }


                  }
                }
            }
            //TODO 3 flush
            MyKafkaUtils.flush()
          }
        )
        //提交offset
        MyOffsetUtils.saveOffset(topicName, group,offsetRanges )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
