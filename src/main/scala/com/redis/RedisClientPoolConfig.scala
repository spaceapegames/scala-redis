package com.redis

import org.apache.commons.pool.impl.GenericObjectPool

trait RedisClientPoolConfig {
  val maxIdle: Int
}

case class RedisStackPoolConfig(maxIdle: Int = 8) extends RedisClientPoolConfig {
}

case class RedisGenericPoolConfig(maxIdle: Int = 8, maxActive: Int = 50000, val whenExhaustedAction: Byte = GenericObjectPool.WHEN_EXHAUSTED_BLOCK, maxWait: Int = 5000) extends RedisClientPoolConfig {

}