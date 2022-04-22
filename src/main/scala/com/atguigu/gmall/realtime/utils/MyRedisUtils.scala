package com.atguigu.gmall.realtime.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis工具类，用于获取Jedis连接
 */
object MyRedisUtils {

  var jedisPool: JedisPool = null
  def getJedis() : Jedis = {
    if(jedisPool == null) {
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      val host : String = MyPropsUtils(MyConfig.REDIS_HOST)
      val port : String = MyPropsUtils(MyConfig.REDIS_PORT)
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }

  /**
   * 关闭连接对象
   */
  def close(jedis: Jedis)  {
    if(jedis != null) jedis
  }
}
