package com.redis.sentinel

import com.redis._
import com.redis.RedisGenericPoolConfig

class RedisClientPoolBySentinel(val masterName: String, val sentinelCluster: SentinelCluster, val maxIdle: Int = 8, val database: Int = 0, val secret: Option[Any] = None, poolConfig: RedisClientPoolConfig = RedisGenericPoolConfig()) extends RedisClientPool
  with SentinelMonitoredRedisMaster{

  private var pool: RedisClientPoolByAddress = null

  sentinelCluster.addMonitoredRedisMaster(this)
  init

  private def init {
    val redisNode = sentinelCluster.getMasterNode(masterName).getOrElse(throw new RedisMasterNotFoundException(masterName))
    pool = new RedisClientPoolByAddress(redisNode.host, redisNode.port, maxIdle, database, secret, poolConfig)
  }
  def poolName: String = masterName

  def withClient[T](body: RedisClient => T) = {
    pool.withClient(body(_))
  }

  def getNode: RedisNode = pool.getNode

  def getMasterName: String = masterName
  def onMasterChange (newRedisNode: RedisNode) {
    updatePool(getNode.copy(host = newRedisNode.host, port = newRedisNode.port))
  }
  def onMasterHeartBeat (newRedisNode: RedisNode) {
    updatePool(getNode.copy(host = newRedisNode.host, port = newRedisNode.port))
  }

  private def updatePool(redisNode: RedisNode){
    if (pool.isRedisNode(redisNode)){
      return
    }
    val newPool = new RedisClientPoolByAddress(redisNode.host, redisNode.port, maxIdle, database, secret, poolConfig)
    val oldPool = pool
    pool = newPool
    oldPool.close
  }

  def close {
    sentinelCluster.removeMonitoredRedisMaster(masterName)
    pool.close
  }
}

