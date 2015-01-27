package com.redis.sentinel

import com.redis._
import org.apache.commons.pool.PoolableObjectFactory

class SentinelClientPool (val node: RedisNode, val poolConfig: RedisClientPoolConfig) extends RedisPoolByAddressBase[SentinelClient] {
  protected def newClientFactory: PoolableObjectFactory[SentinelClient] = new SentinelClientFactory(node)

  def this(addr: SentinelAddress, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()){
    this(RedisNode(addr.host + ":" + String.valueOf(addr.port), addr.host, addr.port), poolConfig)
  }
  def this(host: String, port: Int, maxIdle: Int = 8, database: Int = 0, secret: Option[Any] = None, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()) {
    this(RedisNode(host + ":" + String.valueOf(port), host, port, maxIdle, database, secret), poolConfig)
  }
}
