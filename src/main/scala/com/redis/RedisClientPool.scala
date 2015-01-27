package com.redis

import org.apache.commons.pool.PoolableObjectFactory
import org.apache.commons.pool.impl.{StackObjectPool, GenericObjectPool}

trait RedisClientPool {
  def poolName: String
  def withClient[T](body: RedisClient => T): T
  def close
  def getNode: RedisNode
}

class RedisClientPoolByAddress (val node: RedisNode, val poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()) extends RedisPoolByAddressBase[RedisClient] with RedisClientPool{
  protected def newClientFactory: PoolableObjectFactory[RedisClient] = new RedisClientFactory(node)

  def this(host: String, port: Int, maxIdle: Int = 8, database: Int = 0, secret: Option[Any] = None, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()) {
    this(RedisNode(host + ":" + String.valueOf(port), host, port, maxIdle, database, secret), poolConfig)
  }
}
trait RedisPoolByAddressBase[R <: Redis]
  {
  val node: RedisNode
  val poolConfig: RedisClientPoolConfig
  protected def newClientFactory: PoolableObjectFactory[R]

  val pool = initPool
  override def toString = node.host + ":" + String.valueOf(node.port)
  def poolName: String = node.name
  def getNode: RedisNode = node


  private def initPool = {
    if (poolConfig.isInstanceOf[RedisGenericPoolConfig]) {
      val genericConfig = poolConfig.asInstanceOf[RedisGenericPoolConfig]
      new GenericObjectPool(newClientFactory, genericConfig.maxActive, genericConfig.whenExhaustedAction, genericConfig.maxWait, genericConfig.maxIdle)
    }
    else new StackObjectPool(newClientFactory, poolConfig.maxIdle)
  }
  def withClient[T](body: R => T) = {
    val client = pool.borrowObject
    try {
      body(client)
    } finally {
      pool.returnObject(client)
    }
  }

  // close pool & free resources
  def close = pool.close

  def isRedisNode (otherNode: RedisNode): Boolean = {
    if (otherNode.host != node.host) return false
    if (otherNode.port != node.port) return false
    return true
  }
}

trait PoolCreationByAddress {
  def poolCreator (node: RedisNode, poolConfig: RedisClientPoolConfig): RedisClientPool = {
    new RedisClientPoolByAddress(node, poolConfig)
  }
}