package com.atguigu.gmall.realtime.utils

import java.util.ResourceBundle

/**
 * 配置文件解析工具类
 */
object MyPropsUtils {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(key : String) : String = {
    bundle.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(MyPropsUtils("kafka.bootstrap.server"))
  }
}
