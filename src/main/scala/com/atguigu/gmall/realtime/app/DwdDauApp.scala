package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.utils.MyKafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日活宽表
 * 1。准备实时环境
 * 2. 从Redis中读取offset
 * 3. 从kafka中消费数据
 * 4. 提取offset
 * 5. 处理数据
 * 6. 提交offset
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //2.

    //3. 从kafka中消费数据
//    MyKafkaUtils.getKafkaDStream(ssc)
    ssc.start()
    ssc.awaitTermination()
  }
}
