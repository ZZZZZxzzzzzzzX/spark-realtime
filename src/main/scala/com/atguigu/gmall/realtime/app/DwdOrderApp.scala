package com.atguigu.gmall.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DwdOrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //2. 从redis中读取offset
    val orderInfoTopicName : String = "DWD_ORDER_INFO_I_1118"
    val orderDeteilOffset : String = "DWD_ORDER_DETAIL_I_1118"
    val group : String = "DWD_ORDER_GROUP"

  }
}
