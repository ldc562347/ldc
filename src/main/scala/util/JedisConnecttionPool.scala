package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnecttionPool {
  val config = new JedisPoolConfig()

  config.setMaxTotal(20)

  config.setMaxIdle(10)

  private val pool = new JedisPool(config,"Hadoop01",6379,10000,"123456")

  def getConnection():Jedis={
    pool.getResource
  }
}
