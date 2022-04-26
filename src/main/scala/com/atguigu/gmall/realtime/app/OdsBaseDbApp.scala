package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.utils.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}


/**
 * 业务数据消费分流
 * 1. 准备实时环境
 * 2. 从Redis中读取offsets
 * 3. 从kafka中消费数据
 * 4. 提取偏移量
 * 5. 处理数据
 * 6. flush  kafka缓冲区
 * 7. 提交偏移量
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ode_base_db_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //2. 从redis中读取偏移量
    val topicName: String = "ODS_BASE_DB_1118"
    val group: String = "ODS_BASE_DB_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, group)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    //3. 从kafka中消费数据
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, group, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDstream(ssc, topicName, group)
    }

    //4. 提取偏移量
    var offsetRanges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5. 处理数据
    //  5.1 转换结构
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val jsonObj: JSONObject = JSON.parseObject(value)
        jsonObj
      }
    )
    //5.2 分流数据
    //    维度数据 -> redis
    //    事实数据 -> kafka

    //声明维度表的清单
    val dimTables: Set[String] = Set[String]("user_info", "base_province", "sku_info")
    val factTables: Set[String] = Set[String]("order_info", "order_detail")

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //提取操作类型
              //1. 进行数据过滤  2.明确当前数据的操作类型，方便进行单独分流
              val tableType: String = jsonObj.getString("type")
              val operType: String = tableType match {
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              if (operType != null) {
                //提取表名
                val tableName: String = jsonObj.getString("table")
                //分流事实数据
                if (factTables.contains(tableName)) {
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val dwd_topic_name : String = s"DWD_${tableName.toUpperCase()}_$operType"
                  MyKafkaUtils.send(dwd_topic_name, dataObj.toJSONString)
                }
                //分流维度数据
                if (dimTables.contains(tableName)) {

                }
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
