package com.redis.sentinel

import com.redis.RedisNode

case class SentinelClusterStatus(sentinels: Set[SentinelAddress], redisNodes: Map[String, RedisNode]) {

}
