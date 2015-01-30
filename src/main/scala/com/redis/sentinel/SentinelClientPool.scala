package com.redis.sentinel

import com.redis._
import org.apache.commons.pool.PoolableObjectFactory

class SentinelClientPool (val node: RedisNode, val poolConfig: RedisClientPoolConfig) extends RedisPoolByAddressBase[SentinelClient] {
  protected def newClientFactory: PoolableObjectFactory[SentinelClient] = new SentinelClientFactory(node)

  def this(addr: SentinelAddress, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()){
    this(RedisNode(addr.host + ":" + String.valueOf(addr.port), addr.host, addr.port), poolConfig)
  }
}
