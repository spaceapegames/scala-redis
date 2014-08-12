package com.redis

import org.apache.commons.pool.impl.{StackObjectPool, GenericObjectPool}

trait RedisClientPool {
  def poolName: String
  def withClient[T](body: RedisClient => T): T
  def close
  def getNode: RedisNode
}

class RedisClientPoolByAddress(node: RedisNode, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig())
  extends RedisClientPool{

  val pool = initPool
  override def toString = node.host + ":" + String.valueOf(node.port)
  def poolName: String = node.name
  def getNode: RedisNode = node

  def this(host: String, port: Int, maxIdle: Int = 8, database: Int = 0, secret: Option[Any] = None, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()) {
     this(RedisNode(host + ":" + String.valueOf(port), host, port, maxIdle, database, secret), poolConfig)
  }
  private def initPool = {
    if (poolConfig.isInstanceOf[RedisGenericPoolConfig]) {
      val genericConfig = poolConfig.asInstanceOf[RedisGenericPoolConfig]
      new GenericObjectPool(new RedisClientFactory(node), genericConfig.maxActive, genericConfig.whenExhaustedAction, genericConfig.maxWait, genericConfig.maxIdle)
    }
    else new StackObjectPool(new RedisClientFactory(node), poolConfig.maxIdle)
  }
  def withClient[T](body: RedisClient => T) = {
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