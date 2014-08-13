package com.redis.sentinel

case class SentinelClusterConfig (hearthBeatEnabled: Boolean = true , hearthBeatInterval: Int = 1000, sentinelTimeout: Int = 2000)
